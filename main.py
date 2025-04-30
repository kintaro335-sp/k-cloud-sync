# k-cloud-sync
# Copyright(c) Kintaro Ponce
# MIT Licensed
import os
from dotenv import load_dotenv
import json
import requests
from pathlib import Path
from typing import Optional, List, Literal
from pydantic import BaseModel, ValidationError
import logging
from os import path
import math
import copy
from threading import Thread
import time

load_dotenv(path.join(os.getcwd(), '.env'))

# TODO: agregar logs y mensajes de lo que esta pasando

F_100MB = 100 * 1024 * 1024

json_path_env = os.getenv("JSON_PATH",os.getcwd() + "/dirs.json")

logs_path_env = os.getenv("LOGS_PATH",os.getcwd() + "/logs.log")

validate_json = os.getenv("VALIDATE_JSON", "false")

num_threads = int(os.getenv("THREADS", 1))

if num_threads < 1:
  num_threads = 1

logging.basicConfig(filename=logs_path_env, level=logging.INFO, encoding='utf-8', format='%(asctime)s %(levelname)s %(message)s')

dirs_info = {
  "base_url": "",
  "api_key": "",
  "dirs": []
}

dirs_list = []

threads = {}

scopes_needed = ["files:read", "files:create"]

# modelos de validacion del json

class DirSchema(BaseModel):
  remote_path: str
  local_path: str
  sync_mode: Literal["bidirectional", "send", "get"] = 'get'

class ConfigSchema(BaseModel):
  base_url: str
  api_key: str
  dirs: List[DirSchema]

# fin de modelos de validacion del json

def init_json():
  global dirs_info
  with open(json_path_env, "w") as json_file:
    json.dump(dirs_info, json_file)

def load_json():
  global dirs_info
  if os.path.exists(json_path_env):
    with open(json_path_env) as json_file:
      json_data = json.load(json_file)
      try:
        if validate_json == "true":
          config = ConfigSchema(**json_data)
          dirs_info = json.loads(config.model_dump_json())
        else:
          dirs_info = json_data
      except ValidationError as ve:
        print(ve)
        exit(1)
  else:
    print("No json file config found: initializing")
    logging.error("No json file config found: initializing")
    init_json()
    exit(1)

def get_threads_alive() -> int:
  threads_alive = 0
  for i in threads.keys():
    if threads[i].is_alive():
      threads_alive += 1
  return threads_alive

def copy_dirs():
  global dirs_info, dirs_list
  dirs_list = copy.deepcopy(dirs_info['dirs'])

def verify_auth():
  global dirs_info
  base_url = dirs_info["base_url"]
  api_key = dirs_info["api_key"]
  if base_url == "" or api_key == "":
    print("No base url or api key found")
    logging.error("No base url or api key found")
    exit(1)
  resp = requests.get(f"{base_url}/auth?t={api_key}")
  if resp.status_code != 200:
    print("Error al verificar la autenticacion")
    logging.error("Error al verificar la autenticacion")
    return None
  return resp.json()

def verify_scopes():
  global dirs_info
  base_url = dirs_info["base_url"]
  api_key = dirs_info["api_key"]
  resp = requests.get(f"{base_url}/auth/scopes?t={api_key}")
  if resp.status_code != 200:
    print("Error al verificar los scopes")
    logging.error("Error al verificar los scopes")
    return None
  return resp.json()

def eval_scopes(scopes: List[str]):
  global scopes_needed
  scopes_count = len(scopes_needed)
  scopes_counted = 0
  for scope in scopes:
    if scope in scopes_needed:
      scopes_counted += 1
  return scopes_count == scopes_counted

# utils

def get_file_names(list: List[dict]) -> List[str]:
  return [file["name"] for file in list]

# lectura de archivos

def exists_server(path: str) -> bool:
  global dirs_info
  base_url = dirs_info["base_url"]
  api_key = dirs_info["api_key"]
  resp = requests.get(f"{base_url}/files/exists/{path}?t={api_key}")
  if resp.status_code != 200:
    print(f"Error al verificar la existencia del archivo: {path}")
    logging.error(f"Error al verificar la existencia del archivo: {path}")
    return False
  return resp.json()['exists']

def properties_server(path: str) -> Optional[dict]:
  global dirs_info
  base_url = dirs_info["base_url"]
  api_key = dirs_info["api_key"]
  resp = requests.get(f"{base_url}/files/properties/{path}?t={api_key}")
  if resp.status_code != 200:
    print(f"Error al obtener las propiedades del archivo: {path}")
    logging.error(f"Error al obtener las propiedades del archivo: {path}")
    return {}
  return resp.json()

def file_list_server(path: str) -> List[dict]:
  global dirs_info
  base_url = dirs_info["base_url"]
  api_key = dirs_info["api_key"]
  resp = requests.get(f"{base_url}/files/list/{path}?t={api_key}")
  if resp.status_code != 200:
    print(f"Error al obtener la lista de archivos: {path}")
    logging.error(f"Error al obtener la lista de archivos: {path}")
    return []
  return resp.json()['list']

def download_file_server(path_server: str, local_path: str):
  global dirs_info
  base_url = dirs_info["base_url"]
  api_key = dirs_info["api_key"]
  resp = requests.get(f"{base_url}/files/list/{path_server}?t={api_key}", stream=True)
  if resp.status_code != 200:
    print(f"Error al descargar el archivo: {path_server}")
    logging.error(f"Error al descargar el archivo: {path_server}")
    return None
  with open(Path(local_path), 'wb') as f:
    for chunk in resp.iter_content(chunk_size=8192):
      f.write(chunk)
  return resp

# escritura de archivos

def create_dir_server(path: str):
  global dirs_info
  base_url = dirs_info["base_url"]
  api_key = dirs_info["api_key"]
  resp = requests.post(f"{base_url}/files/folder/{path}?t={api_key}")
  if resp.status_code in [200, 201]:
    return None
  return resp.json()

def upload_big_file_server(path_server: str, file_path: Path):
  global dirs_info
  base_url = dirs_info["base_url"]
  api_key = dirs_info["api_key"]
  file_stream = open(file_path, 'rb')
  file_size = os.path.getsize(file_path)
  resp_init = requests.post(f"{base_url}/files/initialize/{path_server}?t={api_key}", json={'size': file_size})
  if resp_init.status_code not in [200, 201]:
    return resp_init.json()
  num_current_chunk = 0
  num_chunks = math.ceil(file_size / F_100MB)
  for offset in range(0, file_size, F_100MB):
    chunk_size = F_100MB
    file_stream.seek(offset)
    chunk = file_stream.read(chunk_size)
    resp = requests.post(f"{base_url}/files/write/{path_server}?t={api_key}&pos={offset}", files={ 'file': (file_path.name, chunk) })
    if resp.status_code in [200, 201]:
      num_current_chunk += 1
      print(f"Chunk {num_current_chunk}/{num_chunks} uploaded")
    else:
      print(f"Chunk {num_current_chunk}/{num_chunks} failed")

  return { "message": "ok" }

def upload_file_server(path_server: str, file_path: Path):
  global dirs_info
  base_url = dirs_info["base_url"]
  api_key = dirs_info["api_key"]
  file_size = os.path.getsize(file_path)
  if file_size > F_100MB:
    return upload_big_file_server(path_server, file_path)
  files = {'file': open(file_path, 'rb')}
  resp = requests.post(f"{base_url}/files/upload/{path_server}?t={api_key}", files=files)
  if resp.status_code not in [200, 201]:
    return None
  return resp.json()

def read_file_local(path: str):
  return open(path, "rb")

def exists_local(path: Path):
  return os.path.exists(path)

def create_dir_local(path: str):
  Path(path).mkdir(parents=True, exist_ok=True)

def list_dir_local(path: str):
  return Path(path).glob("*")

def sync_get_data(data: dict, virtual_path: str = ""):
  remote_path = data["remote_path"]
  local_path = data["local_path"]
   
  remote_virtual_path = path.join(remote_path, virtual_path)
  local_virtual_path = path.join(local_path, virtual_path)

  files_server = file_list_server(remote_virtual_path)
  total_files = len(list(files_server))

  if virtual_path == "" and not exists_local(local_virtual_path):
    create_dir_local(local_virtual_path)

  for i, file in enumerate(file_list_server(remote_virtual_path)):
    print(f"[{i + 1}/{total_files}] Syncing {file.get('name')}...")
    file_virtual_path_server = path.join(remote_virtual_path, file.get("name"))
    file_virtual_path_local = path.join(local_virtual_path, file.get("name"))
    if file.get("type") == "folder":
      if not exists_local(file_virtual_path_local):
        create_dir_local(file_virtual_path_local)
      sync_get_data(data, path.join(virtual_path, file.get("name")))
    elif file.get("type") == "file":
      if not exists_local(file_virtual_path_local):
        print(f"[{i + 1}/{total_files}] Downloading file...")
        download_file_server(file_virtual_path_server, file_virtual_path_local)
        print(f"[{i + 1}/{total_files}] File downloaded")
      else:
        print(f"[{i + 1}/{total_files}] File already exists")

def sync_send_data(data: dict, virtual_path: str = ""):
  remote_path = data["remote_path"]
  local_path = data["local_path"]

  remote_virtual_path = path.join(remote_path, virtual_path)
  local_virtual_path = path.join(local_path, virtual_path)

  files_server = file_list_server(remote_virtual_path)
  files_names_server = get_file_names(files_server)
  
  files_local = list_dir_local(local_virtual_path)

  total_files = len(list(files_local))

  for i, file in enumerate(list_dir_local(local_virtual_path)):
    print(f"[{i + 1}/{total_files}] Syncing {file.name}...")
    file_virtual_path_server = path.join(remote_virtual_path, file.name)
    if file.is_dir():
      print("folder")
      if not exists_server(file_virtual_path_server):
          create_dir_server(file_virtual_path_server)
      else:
          file_server_props = properties_server(file_virtual_path_server)
          if file_server_props.get("type") != "folder":
            return
      sync_send_data(data, path.join(virtual_path, file.name))
    else:
      if file.name not in files_names_server:
        print(f"[{i + 1}/{total_files}] Uploading:{file.name}")
        upload_file_server(file_virtual_path_server, file)
        print(f"[{i + 1}/{total_files}] File:{file.name} uploaded")
      else:
        print(f"[{i + 1}/{total_files}] File:{file.name} already exists")

def sync_dir(data: dict):
  remote_path = data["remote_path"]
  local_path = data["local_path"]
  print(f"Syncing {local_path}")
  sync_mode = data["sync_mode"]
  exists = exists_server(remote_path)
  props = properties_server(remote_path)
  if props.get("type") == "file":
    print("it is a file, not a directory")
    logging.error("it is a file, not a directory")
    return
  if not exists:
    create_dir_server(remote_path)
 
  if sync_mode in ["send", "bidirectional"]:
    print("Sending data...")
    sync_send_data(data)
  if sync_mode in ["get", "bidirectional"]:
    print("Getting data...")
    sync_get_data(data)

def main_single_thread():
  global dirs_list
  logging.info("started")
  print("started")
  for dir in dirs_list:
    sync_dir(dir)
  logging.info("Sync finished")
  print("Sync finished")

def sync_dir_thread(thread_id: int):
  global dirs_list
  while len(dirs_list) != 0:
    try:
      dir_info = dirs_list.pop()
      sync_dir(dir_info)
    except IndexError:
      break
  print(f"thread {thread_id} finished")
  logging.info(f"thread {thread_id} finished")

def start_threads():
  global threads, num_threads
  for i in range(0, num_threads):
    threads[i] = Thread(target=sync_dir_thread, args=(i,), daemon=False)
    threads[i].start()
    print(f"thread {i} started")
    logging.info(f"thread {i} started")
  threads_alive = get_threads_alive()
  while threads_alive > 0:
    print(f"threads alive {threads_alive}/{num_threads}")
    logging.info(f"threads alive {threads_alive}/{num_threads}")
    time.sleep(5)
  logging.info("sync finished")
  print("sync finished")

def main():
  global dirs_list
  logging.info("program started")
  print("program started")
  logging.info("loading data")
  print("loading data")
  load_json()
  logging.info("data loaded")
  print("data loaded")
  logging.info("authenticathing")
  print("authenticathing")
  auth = verify_auth()
  if auth == None:
    exit(1)
  logging.info("authenticathed")
  print("authenticathend")
  logging.info("verifying scopes")
  print("verifying scopes")
  scopes = verify_scopes()
  if scopes == None:
    exit(1)
  if not eval_scopes(scopes["scopes"]):
    print("No tienes los permisos suficientes para sincronizar")
    logging.error("No tienes los permisos suficientes para sincronizar")
    exit(1)
  logging.info("scopes verified")
  print("scopes verified")
  logging.info("loading dirs")
  print("loading dirs")
  copy_dirs()
  logging.info("dirs loaded")
  print("dirs loaded")
  if threads == 1 or len(dirs_list) == 1:
    logging.info("starting with 1 thread")
    print("starting with 1 thread")
    main_single_thread()
  else:
    logging.info(f"starting with {num_threads} threads")
    print(f"starting with {num_threads} threads")
    start_threads()

if __name__ == "__main__":
  main()
