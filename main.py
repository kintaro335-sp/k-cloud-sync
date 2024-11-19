import os
from dotenv import load_dotenv
load_dotenv()
import json
import requests
from pathlib import Path

from os import path

F_100MB = 100 * 1024 * 1024

json_path_env = os.getenv("JSON_PATH",os.getcwd() + "/dirs.json")

dirs_info = {
  "base_url": "",
  "api_key": "",
  "dirs": []
}

def load_json():
  global dirs_info
  if os.path.exists(json_path_env):
    with open(json_path_env) as json_file:
      dirs_info = json.load(json_file)
  else:
    print("No json file config found")
    exit(1)

# TODO: agregar funciones de comnuicacion con el servidor

def verify_auth():
  base_url = dirs_info["base_url"]
  api_key = dirs_info["api_key"]
  if base_url == "" or api_key == "":
    print("No base url or api key found")
    exit(1)
  resp = requests.get(f"{base_url}/auth?t={api_key}")
  if resp.status_code != 200:
    return None
  return resp.json()

# lectura de archivos

def exists_server(path: str):
  base_url = dirs_info["base_url"]
  api_key = dirs_info["api_key"]
  resp = requests.get(f"{base_url}/files/exists/{path}?t={api_key}")
  if resp.status_code != 200:
    return None
  return resp.json()['exists']

def properties_server(path: str):
  base_url = dirs_info["base_url"]
  api_key = dirs_info["api_key"]
  resp = requests.get(f"{base_url}/files/properties/{path}?t={api_key}")
  if resp.status_code != 200:
    return None
  return resp.json()

def file_list_server(path: str):
  base_url = dirs_info["base_url"]
  api_key = dirs_info["api_key"]
  resp = requests.get(f"{base_url}/files/list/{path}?t={api_key}")
  if resp.status_code != 200:
    return None
  return resp.json()

def download_file_server(path_server: str, local_path: str):
  base_url = dirs_info["base_url"]
  api_key = dirs_info["api_key"]
  resp = requests.get(f"{base_url}/files/list/{path_server}?t={api_key}", stream=True)
  with open(Path(f"{local_path}/{path_server}"), 'wb') as f:
    for chunk in resp.iter_content(chunk_size=8192):
      f.write(chunk)
  if resp.status_code != 200:
    return None
  return resp

# escritura de archivos

def create_dir_server(path: str):
  base_url = dirs_info["base_url"]
  api_key = dirs_info["api_key"]
  resp = requests.post(f"{base_url}/files/folder/{path}?t={api_key}")
  if resp.status_code in [200, 201]:
    return None
  return resp.json()

def upload_big_file_server(path_server: str, file_path: Path):
  base_url = dirs_info["base_url"]
  api_key = dirs_info["api_key"]
  file_stream = open(file_path, 'rb')
  file_size = os.path.getsize(file_path)
  resp_init = requests.post(f"{base_url}/files/initialize/{path_server}?t={api_key}", json={'size': file_size})
  if resp_init.status_code in [200, 201]:
    return None
  offset = 0
  while offset < file_size:
    chunk_size = F_100MB
    file_stream.seek(offset)
    chunk = file_stream.read(chunk_size)
    resp = requests.post(f"{base_url}/files/write/{path_server}?t={api_key}&pos={offset}", files={ 'file': (file_path.name, chunk) })
    offset += chunk_size

  return { "message": "ok" }

def upload_file_server(path_server: str, file_path: Path):
  base_url = dirs_info["base_url"]
  api_key = dirs_info["api_key"]
  file_size = os.path.getsize(file_path)
  if file_size > F_100MB:
    return upload_big_file_server(path_server, file_path)
  files = {'file': open(file_path, 'rb')}
  resp = requests.post(f"{base_url}/files/upload/{path_server}?t={api_key}", files=files)
  if resp.status_code in [200, 201]:
    return None
  return resp.json()


# TODO: agregar funciones de fs para archivos locales

def read_file_local(path: str):
  return open(path, "rb")

def exists_local(path: str):
  return os.path.exists(path)

def create_dir_local(path: str):
  Path(path).mkdir(parents=True, exist_ok=True)

def list_dir_local(path: str):
  return Path(path).iterdir()

# TODO: agrega funciones que hagan tareas que involucran las funciones de arriba



# TODO: hacer el primer tipo de rutina del tipo send, enviar archivos al servidos.

def sync_get_data(data: dict, virtual_path: str = ""):
  remote_path = data["remote_path"]
  local_path = data["local_path"]
 
  
  remote_virtual_path = path.join(remote_path, virtual_path)
  local_virtual_path = path.join(local_path, virtual_path)

  for file in list_dir_local(local_virtual_path):
    file_virtual_path_server = path.join(remote_virtual_path, file.name)
    file_virtual_path_local = path.join(local_virtual_path, file.name)
    if file.is_dir():
      if exists_server(file_virtual_path_server):
        file_server_props = properties_server(file_virtual_path_server)
        if file_server_props.get("type") == "folder":
          create_dir_local(file_virtual_path_server)
      sync_get_data(data, file_virtual_path_server)
    else:
      if exists_server(file_virtual_path_server):
        download_file_server(file_virtual_path_server, file_virtual_path_local)
  

def sync_send_data(data: dict, virtual_path: str = ""):
  remote_path = data["remote_path"]
  local_path = data["local_path"]

  remote_virtual_path = path.join(remote_path, virtual_path)
  local_virtual_path = path.join(local_path, virtual_path)

  for file in list_dir_local(local_virtual_path):
    file_virtual_path_server = path.join(remote_virtual_path, file.name)
    if file.is_dir():
      if exists_server(file_virtual_path_server):
        file_server_props = properties_server(file_virtual_path_server)
        if file_server_props.get("type") == "folder":
          create_dir_server(file_virtual_path_server)
      sync_send_data(data, file_virtual_path_server)
    else:
      if not exists_server(file_virtual_path_server):
        upload_file_server(file_virtual_path_server, file)

def sync_dir(data: dict):
  remote_path = data["remote_path"]
  local_path = data["local_path"]
  sync_mode = data["sync_mode"]
  exists = exists_server(remote_path)
  props = properties_server(remote_path)
  if props.get("type") == "file":
    print("it is a file, not a directory")
    return
  if exists:
    pass
  else:
    create_dir_server(remote_path)
  
  if sync_mode in ["send", "bidirectional"]:
    sync_send_data(data)
  if sync_mode in ["get", "bidirectional"]:
    sync_get_data(data)

def main():
  load_json()
  auth = verify_auth()
  if auth == None:
    exit(1)
  for dir in dirs_info["dirs"]:
    sync_dir(dir)


if __name__ == "__main__":
  main()
