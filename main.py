import os.path
import random
import time
from multiprocessing import Pool, cpu_count
import shutil
from loguru import logger
import sys

from google.oauth2.service_account import Credentials

from GAdmin import GAdmin
from GDrive import GDrive
from Compressor import Compressor

MAX_DOWNLOAD_THREADS = int(os.getenv("MAX_DOWNLOAD_THREADS", 10))
MAX_DRIVE_PROCESSES = int(os.getenv("MAX_DRIVE_PROCESSES", 4))
COMPRESS_DRIVES = os.getenv("COMPRESS_DRIVES", "false").lower() == "true"
COMPRESSION_ALGORITHM = os.getenv("COMPRESSION_ALGORITHM", "pigz")
COMPRESSION_PROCESSES = int(os.getenv("COMPRESSION_PROCESSES", cpu_count()))
DRIVE_WHITELIST = os.getenv("DRIVE_WHITELIST", "").split(",")

SERVICE_ACCOUNT_FILE = os.getenv("SERVICE_ACCOUNT_FILE", "service-account-key.json")
SCOPES = ["https://www.googleapis.com/auth/admin.directory.user.readonly", 
          "https://www.googleapis.com/auth/drive.metadata.readonly",
          "https://www.googleapis.com/auth/drive.readonly"]
DELEGATED_ADMIN_EMAIL = os.getenv("DELEGATED_ADMIN_EMAIL", "xxx")
WORKSPACE_CUSTOMER_ID = os.getenv("WORKSPACE_CUSTOMER_ID", "xxx")

random.seed(time.time())


def get_credentials(subject):
    return Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES).with_subject(subject)

def process_drive(drive: GDrive):
    start_time = time.time()
    drive_id = drive.get_drive_id()

    logger.info(f"({drive_id}) Processing drive")
    
    drive.fetch_file_list()
    logger.debug(f"({drive_id}) Files found: {drive.get_file_list_length()}")
    drive.dump_file_list(f"metadata/{drive_id}.json")
    logger.info(f"({drive_id}) File list saved to metadata/{drive_id}.json")
    
    logger.info(f"({drive_id}) Downloading files")
    drive.download_all_files(f"files/{drive_id}")
    logger.info(f"({drive_id}) Files downloaded")

    if COMPRESS_DRIVES and len(drive.get_file_list()) > 0:
        compressor = Compressor(COMPRESSION_ALGORITHM, delete_original=True, max_processes=COMPRESSION_PROCESSES)
        compressor.compress_folder(f"files/{drive_id}")

    logger.info(f"({drive_id}) Drive processed in {time.time() - start_time:.2f}s")


def main():

    admin_credentials = get_credentials(DELEGATED_ADMIN_EMAIL)
    gadmin = GAdmin(WORKSPACE_CUSTOMER_ID, admin_credentials)

    users = [user["primaryEmail"] for user in gadmin.get_user_list()]
    logger.debug(f"Users found: {users}")
    shared_drives = [drive["id"] for drive in gadmin.get_shared_drives()]
    logger.debug(f"Shared drives found: {shared_drives}")

    drives = []
    for drive_name in users:
        drives.append(GDrive(drive_name, get_credentials(drive_name), "user"))
    for drive_name in shared_drives:
        drives.append(GDrive(drive_name, admin_credentials, "shared"))

    logger.debug(f"Whiltelist: {DRIVE_WHITELIST}")
    logger.debug(f"Drives initialized: {drives}")

    if DRIVE_WHITELIST == [""]:
        logger.warning("No whitelist specified, processing all drives")
    else:
        drives = [drive for drive in drives if drive.get_drive_id() in DRIVE_WHITELIST]

    logger.info(f"Drives to process: {drives}")

    random.shuffle(drives) # In case of failure, every backup will have some unique data
    
    with Pool(processes=MAX_DRIVE_PROCESSES) as pool:
        for _ in pool.imap_unordered(process_drive, drives):
            pass


if __name__ == "__main__":
    logger.remove(0)
    logger.add(sys.stdout, level="DEBUG")
    main()