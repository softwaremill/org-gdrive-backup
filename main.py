import os.path
import random
import time
from tqdm import tqdm
from multiprocessing import Pool, cpu_count
import pgzip
import shutil

from google.auth.transport.requests import Request
from google.oauth2.service_account import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import json
from concurrent.futures import ThreadPoolExecutor, as_completed

MAX_QUERY_THREADS = int(os.getenv("MAX_QUERY_THREADS", 5))
MAX_DOWNLOAD_PROCESSES = int(os.getenv("MAX_DOWNLOAD_PROCESSES", cpu_count()))
FILES_PER_DOWNLOAD_BATCH = int(os.getenv("FILES_PER_DOWNLOAD_BATCH", 1))
COMPRESSION_ALGORITHM = os.getenv("COMPRESSION_ALGORITHM", "pigz")
PIGZ_COMPRESSION_PROCESSES = int(os.getenv("COMPRESSION_PROCESSES", cpu_count()))

SERVICE_ACCOUNT_FILE = os.getenv("SERVICE_ACCOUNT_FILE", "service-account-key.json")
SCOPES = ["https://www.googleapis.com/auth/admin.directory.user.readonly", 
          "https://www.googleapis.com/auth/drive.metadata.readonly",
          "https://www.googleapis.com/auth/drive.readonly"]
DELEGATED_ADMIN_EMAIL = os.getenv("DELEGATED_ADMIN_EMAIL", "xxx")
WORKSPACE_CUSTOMER_ID = os.getenv("WORKSPACE_CUSTOMER_ID", "xxx")
COMPRESS_DRIVES = os.getenv("COMPRESS_DRIVES", "false").lower() == "true"

random.seed(time.time())


def get_credentials(subject):
    return Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES).with_subject(subject)
  
def get_users(admin_creds):
    users = []
    service = build("admin", "directory_v1", credentials=admin_creds)
    request = service.users().list(customer=WORKSPACE_CUSTOMER_ID, maxResults=100, orderBy="email")
    while request is not None:
        response = request.execute()
        users.extend(response.get("users", []))
        request = service.users().list_next(request, response)
    return users

def get_files(drive_service, drive_id=None):
    files = []
    if drive_id:
        return get_files_shared(drive_service, drive_id)
    else:
        return get_files_user(drive_service)

def get_files_user(drive_service):
    files = []
    request = drive_service.files().list(pageSize=100, fields="nextPageToken, files(id, name, md5Checksum, parents, mimeType, shortcutDetails, permissions)")
    while request is not None:
        response = request.execute()
        files.extend(response.get("files", []))
        request = drive_service.files().list_next(request, response)
    return files

def get_files_shared(drive_service, drive_id):
    files = []
    known_permissions = {}
    request = drive_service.files().list(pageSize=100, fields="nextPageToken, files(id, name, md5Checksum, parents, mimeType, shortcutDetails, permissionIds)", corpora="drive", driveId=drive_id, includeItemsFromAllDrives=True, supportsAllDrives=True)
    while request is not None:
        response = request.execute()
        files.extend(response.get("files", []))
        request = drive_service.files().list_next(request, response)
    for f in files:
        f["permissions"] = []
        if "permissionIds" in f:
            for permission_id in f["permissionIds"]:
                if permission_id in known_permissions:
                    f["permissions"].append(known_permissions[permission_id])
                else:
                    permission = drive_service.permissions().get(fileId=f["id"], permissionId=permission_id, fields="id, displayName, type, kind, emailAddress, role", supportsAllDrives=True).execute()
                    f["permissions"].append(permission)
                    known_permissions[permission_id] = permission
    return files

def get_file_path(drive_service, file_id, supportsAllDrives=False):
    file_path = []

    file = drive_service.files().get(fileId=file_id, fields="name, parents", supportsAllDrives=supportsAllDrives).execute()
    file_path.append(file['name'])

    parents = file.get('parents', [])
    while parents:
        parent_id = parents[0]
        parent = drive_service.files().get(fileId=parent_id, fields="name, parents", supportsAllDrives=supportsAllDrives).execute()
        #check if parent is a shared drive
        if "parents" not in parent:
            # get shared drive name
            drive = drive_service.drives().get(driveId=parent_id).execute()
            file_path.append(drive['name'])
            break
        file_path.append(parent['name'])
        parents = parent.get('parents', [])

    return "/".join(reversed(file_path))

def update_file_path(file, credentials):
    drive_service = build("drive", "v3", credentials=credentials)
    file["path"] = get_file_path(drive_service, file["id"])
    return file

def find_file_by_id(files, file_id):
    for file in files:
        if file["id"] == file_id:
            return file
    return None

def build_file_path(files, file_id):
    file = find_file_by_id(files, file_id)
    if file is None:
        return None
    file_path = []
    while "parents" in file:
        parent_id = file["parents"][0]
        parent = find_file_by_id(files, parent_id)
        if parent is None:
            break
        file_path.append(parent["name"])
        file = parent
    return "/".join(reversed(file_path))

def save_user_file_list(user_email):
    credentials = get_credentials(user_email)
    drive_service = build("drive", "v3", credentials=credentials)
    files = get_files(drive_service)
    print("Files found:", len(files), f"({user_email})")
    save_file_list(f"u_{user_email}", files)
    

def save_shared_file_list(drive_id):
    credentials = get_credentials(DELEGATED_ADMIN_EMAIL)
    drive_service = build("drive", "v3", credentials=credentials)
    files = get_files(drive_service, drive_id=drive_id)
    print("Files found:", len(files), f"({drive_id})")
    save_file_list(f"s_{drive_id}", files)

def setup_folders():
    if not os.path.exists("metadata"):
        os.makedirs("metadata")
    if not os.path.exists("files"):
        os.makedirs("files")

def save_file_list(file_name, files):
    for i, f in enumerate(files):
        f["path"] = build_file_path(files, f["id"])
        files[i] = f
    with open(f"metadata/{file_name}.json", "w") as f:
        f.write(json.dumps(files, indent=4))

def get_shared_drives(admin_creds):
    drives = []
    service = build("drive", "v3", credentials=admin_creds)
    request = service.drives().list(pageSize=100)
    while request is not None:
        response = request.execute()
        drives.extend(response.get("drives", []))
        request = service.drives().list_next(request, response)
    return drives
    

def download_drive_file(file, drive_id, drive_service):
    request = drive_service.files().get_media(fileId=file['id'])
    os.makedirs(f"files/{drive_id}/{file['path']}", exist_ok=True)
    with open(f"files/{drive_id}/{file['path']}/{file['name']}", "wb") as f:
        f.write(request.execute())

def export_drive_file(file, drive_id, drive_service):
    if file['mimeType'] == "application/vnd.google-apps.folder":
        return
    
    match file['mimeType']:
        case "application/vnd.google-apps.shortcut":
            original_file = file['shortcutDetails']['targetId']
            original_file_path = get_file_path(drive_service, original_file, supportsAllDrives=True)
            with open(f"files/{drive_id}/{file['path']}/{file['name']}.lnk.txt", "w") as f:
                f.write(original_file_path)
        case "application/vnd.google-apps.document":
            request = drive_service.files().export_media(fileId=file['id'], mimeType="application/vnd.openxmlformats-officedocument.wordprocessingml.document")
            os.makedirs(f"files/{drive_id}/{file['path']}", exist_ok=True)
            with open(f"files/{drive_id}/{file['path']}/{file['name']}.docx", "wb") as f:
                f.write(request.execute())
        case "application/vnd.google-apps.spreadsheet":
            request = drive_service.files().export_media(fileId=file['id'], mimeType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
            os.makedirs(f"files/{drive_id}/{file['path']}", exist_ok=True)
            with open(f"files/{drive_id}/{file['path']}/{file['name']}.xlsx", "wb") as f:
                f.write(request.execute())
        case "application/vnd.google-apps.presentation":
            request = drive_service.files().export_media(fileId=file['id'], mimeType="application/vnd.openxmlformats-officedocument.presentationml.presentation")
            os.makedirs(f"files/{drive_id}/{file['path']}", exist_ok=True)
            with open(f"files/{drive_id}/{file['path']}/{file['name']}.pptx", "wb") as f:
                f.write(request.execute())
        case "application/vnd.google-apps.drawing":
            request = drive_service.files().export_media(fileId=file['id'], mimeType="application/pdf")
            os.makedirs(f"files/{drive_id}/{file['path']}", exist_ok=True)
            with open(f"files/{drive_id}/{file['path']}/{file['name']}.pdf", "wb") as f:
                f.write(request.execute())
        case "application/vnd.google-apps.script":
            request = drive_service.files().export_media(fileId=file['id'], mimeType="application/vnd.google-apps.script+json")
            os.makedirs(f"files/{drive_id}/{file['path']}", exist_ok=True)
            with open(f"files/{drive_id}/{file['path']}/{file['name']}.json", "wb") as f:
                f.write(request.execute())
        case _:
            with open(f"files/{drive_id}/errors.txt", "a") as f:
                f.write(f"Unknown file type: {file['mimeType']} ({file['id']})\n")

def process_file(args):
    batch, drive_id, credentials = args
    drive_service = build("drive", "v3", credentials=credentials)
    for file in batch:
        try:
            if 'md5Checksum' in file: # check if file is binary
                download_drive_file(file, drive_id, drive_service)
            else:
                export_drive_file(file, drive_id, drive_service)
        except Exception as e:
            with open(f"files/{drive_id}/errors.txt", "a") as f:
                f.write(f"Error processing file {file['id']}: {str(e)}\n")

def chunkify(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

def download(compress=False):
    drives = os.listdir("metadata")
    random.shuffle(drives) # In case of failure, we should have at least some data

    for drive in drives:
        with open(f"metadata/{drive}", "r") as f:
            files = json.load(f)

        drive_id = drive[2:-5]
        credentials = get_credentials(DELEGATED_ADMIN_EMAIL)
        with Pool(processes=MAX_DOWNLOAD_PROCESSES) as pool:
            args = [(batch, drive_id, credentials) for batch in chunkify(files, FILES_PER_DOWNLOAD_BATCH)]
            with tqdm(total=len(files), desc=f"Downloading files for {drive_id}") as pbar:
                for _ in pool.imap_unordered(process_file, args):
                    pbar.update(FILES_PER_DOWNLOAD_BATCH)
        
        if compress and len(files) > 0:
            compress_drive(drive_id, COMPRESSION_ALGORITHM)
            # compress_folder(f"metadata/{drive}", "lz4")

def compress_drive(drive_id, algorithm="lz4"):
    print(f"Compressing {drive_id} with {algorithm}")
    start_time = time.time()
    original_size = os.path.getsize(f"files/{drive_id}")

    match algorithm:
        case "lz4":
            tar_path = f"files/{drive_id}.tar.lz4"
            success = os.system(f"tar c - -C files {drive_id} | lz4 - {tar_path}")
        case "pigz":
            tar_path = f"files/{drive_id}.tar.gz"
            success = os.system(f"tar cf - -C files {drive_id} | pigz -p {PIGZ_COMPRESSION_PROCESSES} > {tar_path}")
        case _:
            raise NotImplementedError(f"Compression algorithm {algorithm} not implemented")
        
    shutil.rmtree(f"files/{drive_id}")
        
    print(f"Compression finished in {time.time() - start_time:.2f}s")
    compressed_size = os.path.getsize(tar_path)
    print(f"Compression ratio: {original_size / compressed_size:.2f}")



def main():
    setup_folders()

    admin_creds = get_credentials(DELEGATED_ADMIN_EMAIL)
    users = [user["primaryEmail"] for user in get_users(admin_creds)]
    shared_drives = [drive["id"] for drive in get_shared_drives(admin_creds)]

    with ThreadPoolExecutor(max_workers=MAX_QUERY_THREADS) as executor:
        futures = [executor.submit(save_user_file_list, user) for user in users]
    with ThreadPoolExecutor(max_workers=MAX_QUERY_THREADS) as executor:
        futures = [executor.submit(save_shared_file_list, drive) for drive in shared_drives]

    download(compress=COMPRESS_DRIVES)


if __name__ == "__main__":
  main()