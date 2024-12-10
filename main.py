import os.path
import random
import time
from multiprocessing import Pool, cpu_count
from logger import app_logger as logger

from google.oauth2.service_account import Credentials

from GAdmin import GAdmin
from GDrive import GDrive
from S3 import S3
from Compressor import Compressor

MAX_DOWNLOAD_THREADS = int(os.getenv("MAX_DOWNLOAD_THREADS", 20))
MAX_DRIVE_PROCESSES = int(os.getenv("MAX_DRIVE_PROCESSES", 4))
COMPRESS_DRIVES = os.getenv("COMPRESS_DRIVES", "false").lower() == "true"
COMPRESSION_ALGORITHM = os.getenv("COMPRESSION_ALGORITHM", "pigz")
COMPRESSION_PROCESSES = int(os.getenv("COMPRESSION_PROCESSES", cpu_count()))
DRIVE_WHITELIST = os.getenv("DRIVE_WHITELIST", "").split(",")

SERVICE_ACCOUNT_FILE = os.getenv("SERVICE_ACCOUNT_FILE", "service-account-key.json")
SCOPES = ["https://www.googleapis.com/auth/admin.directory.user.readonly", 
          "https://www.googleapis.com/auth/drive.metadata.readonly",
          "https://www.googleapis.com/auth/drive.readonly"]
DELEGATED_ADMIN_EMAIL = os.getenv("DELEGATED_ADMIN_EMAIL")
WORKSPACE_CUSTOMER_ID = os.getenv("WORKSPACE_CUSTOMER_ID")

S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY")

random.seed(time.time())


def get_credentials(subject):
    return Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES).with_subject(subject)

def process_drive(args):
    drive, current_timestamp = args
    start_time = time.time()
    drive_id = drive.get_drive_id()
    downloads_path = f"downloads/{current_timestamp}/{drive_id}"
    metadata_path = f"{downloads_path}/files.json"
    files_path = f"{downloads_path}/files"

    logger.info(f"({drive_id}) Processing drive")
    
    drive.fetch_file_list()
    logger.debug(f"({drive_id}) Files found: {drive.get_file_list_length()}")
    drive.dump_file_list(metadata_path)
    logger.info(f"({drive_id}) File list saved to {metadata_path}")
    
    logger.info(f"({drive_id}) Downloading files")
    drive.download_all_files(files_path, threads=MAX_DOWNLOAD_THREADS)
    logger.info(f"({drive_id}) Files downloaded")

    if COMPRESS_DRIVES and len(drive.get_file_list()) > 0:
        logger.info(f"({drive_id}) Compressing files")
        compress_time_start = time.time()
        compressor = Compressor(COMPRESSION_ALGORITHM, delete_original=True, max_processes=COMPRESSION_PROCESSES)
        _, tar_size = compressor.compress_folder(files_path)
        logger.info(f"({drive_id}) Files compressed in {time.time() - compress_time_start:.2f}s ({tar_size/1024/1024:.2f}MB)")

    if len(drive.get_file_list()) == 0:
        logger.warning(f"({drive_id}) No files found, skipping upload")
    else:
        s3 = S3(S3_BUCKET_NAME, S3_ACCESS_KEY, S3_SECRET_KEY)
        logger.info(f"({drive_id}) Uploading files to S3")
        upload_time_start = time.time()
        s3.upload_folder(downloads_path, f"{current_timestamp}/{drive_id}")
        logger.info(f"({drive_id}) Files uploaded in {time.time() - upload_time_start:.2f}s")

    logger.info(f"({drive_id}) Drive processed in {time.time() - start_time:.2f}s ({drive.get_file_list_length()} files)")

def validate_env():
    if DELEGATED_ADMIN_EMAIL == "" or DELEGATED_ADMIN_EMAIL is None:
        raise ValueError("DELEGATED_ADMIN_EMAIL is not set")
    if WORKSPACE_CUSTOMER_ID == "" or WORKSPACE_CUSTOMER_ID is None:
        raise ValueError("WORKSPACE_CUSTOMER_ID is not set")
    if S3_BUCKET_NAME == "" or S3_BUCKET_NAME is None:
        raise ValueError("S3_BUCKET_NAME is not set")
    if S3_ACCESS_KEY == "" or S3_ACCESS_KEY is None:
        raise ValueError("S3_ACCESS_KEY is not set")
    if S3_SECRET_KEY == "" or S3_SECRET_KEY is None:
        raise ValueError("S3_SECRET_KEY is not set")


def main():

    validate_env()

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
        current_timestamp = time.strftime("%Y%m%d-%H%M%S")
        logger.debug(f"Current timestamp: {current_timestamp}")

        args = [(drive, current_timestamp) for drive in drives]
        for _ in pool.imap_unordered(process_drive, args):
            pass


if __name__ == "__main__":
    main()