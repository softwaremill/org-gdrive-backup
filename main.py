import os.path
import random
import time
from multiprocessing import Pool, cpu_count
from logger import app_logger as logger
import threading
from pydantic import Field, EmailStr, field_validator
from pydantic_settings import BaseSettings

from google.oauth2.service_account import Credentials

from GAdmin import GAdmin
from GDrive import GDrive
from S3 import S3
from Compressor import Compressor

class Settings(BaseSettings):
    MAX_DOWNLOAD_THREADS: int = Field(20, env="MAX_DOWNLOAD_THREADS")
    MAX_DRIVE_PROCESSES: int = Field(4, env="MAX_DRIVE_PROCESSES")
    COMPRESS_DRIVES: bool = Field(False, env="COMPRESS_DRIVES")
    COMPRESSION_ALGORITHM: str = Field("pigz", env="COMPRESSION_ALGORITHM")
    COMPRESSION_PROCESSES: int = Field(cpu_count(), env="COMPRESSION_PROCESSES")
    DRIVE_WHITELIST: list = Field([], env="DRIVE_WHITELIST")
    SERVICE_ACCOUNT_FILE: str = Field("service-account-key.json", env="SERVICE_ACCOUNT_FILE")
    DELEGATED_ADMIN_EMAIL: EmailStr = Field(None, env="DELEGATED_ADMIN_EMAIL")
    WORKSPACE_CUSTOMER_ID: str = Field(None, env="WORKSPACE_CUSTOMER_ID")
    S3_BUCKET_NAME: str = Field(None, env="S3_BUCKET_NAME")
    S3_ACCESS_KEY: str = Field(None, env="S3_ACCESS_KEY")
    S3_SECRET_KEY: str = Field(None, env="S3_SECRET_KEY")

    @field_validator("MAX_DOWNLOAD_THREADS", "MAX_DRIVE_PROCESSES", "COMPRESSION_PROCESSES")
    def validate_positive_values(cls, v, info):
        if v <= 0:
            raise ValueError(f"{info.field_name} must be positive")
        return v
    
    @field_validator("COMPRESSION_ALGORITHM")
    def validate_compression_algorithm(cls, v, info):
        if v not in ["pigz", "lz4"]:
            raise ValueError(f"{info.field_name} must be 'pigz' or 'lz4'")
        return v
    
    @field_validator("SERVICE_ACCOUNT_FILE")
    def validate_file_exists(cls, v, info):
        if not os.path.exists(v):
            raise ValueError(f"{info.field_name} does not exist")
        return v
    
    @field_validator("WORKSPACE_CUSTOMER_ID", "S3_BUCKET_NAME", "S3_ACCESS_KEY", "S3_SECRET_KEY")
    def validate_not_none(cls, v, info):
        if v is None or v == "":
            raise ValueError(f"{info.field_name} must be set")
        return v
    

SETTINGS = Settings()
SCOPES = ["https://www.googleapis.com/auth/admin.directory.user.readonly", 
          "https://www.googleapis.com/auth/drive.metadata.readonly",
          "https://www.googleapis.com/auth/drive.readonly"]

random.seed(time.time())


def get_credentials(subject):
    return Credentials.from_service_account_file(SETTINGS.SERVICE_ACCOUNT_FILE, scopes=SCOPES).with_subject(subject)

def process_drive(args):
    current_task = "DOWNLOADING"
    drive, current_timestamp = args
    start_time = time.time()
    drive_id = drive.get_drive_id()
    downloads_path = f"downloads/{current_timestamp}/{drive_id}"
    metadata_path = f"{downloads_path}/files.json"
    files_path = f"{downloads_path}/files"

    def print_status():
        counter = 0
        while not stop_event.is_set():
            time.sleep(1)
            counter += 1
            if counter % 60 == 0 and current_task != "DONE":
                logger.info(f"({drive_id}) Current status: {current_task}. Files found: {drive.get_file_list_length()}. Time elapsed: {time.time() - start_time:.2f}s")

    stop_event = threading.Event()
    status_thread = threading.Thread(target=print_status, daemon=True)
    status_thread.start()

    try:
        logger.info(f"({drive_id}) Processing drive")
        
        drive.fetch_file_list()
        logger.debug(f"({drive_id}) Files found: {drive.get_file_list_length()}")
        drive.dump_file_list(metadata_path)
        logger.info(f"({drive_id}) File list saved to {metadata_path}")
        
        logger.info(f"({drive_id}) Downloading files")
        drive.download_all_files(files_path, threads=SETTINGS.MAX_DOWNLOAD_THREADS)
        logger.info(f"({drive_id}) Files downloaded")

        if SETTINGS.COMPRESS_DRIVES and len(drive.get_file_list()) > 0:
            current_task = "COMPRESSING"
            logger.info(f"({drive_id}) Compressing files")
            compress_time_start = time.time()
            compressor = Compressor(SETTINGS.COMPRESSION_ALGORITHM, max_processes=SETTINGS.COMPRESSION_PROCESSES)
            _, tar_size = compressor.compress_folder(files_path, delete_original=True)
            logger.info(f"({drive_id}) Files compressed in {time.time() - compress_time_start:.2f}s ({tar_size/1024/1024:.2f}MB)")

        if len(drive.get_file_list()) == 0:
            logger.warning(f"({drive_id}) No files found, skipping upload")
        else:
            current_task = "UPLOADING"
            s3 = S3(SETTINGS.S3_BUCKET_NAME, SETTINGS.S3_ACCESS_KEY, SETTINGS.S3_SECRET_KEY)
            logger.info(f"({drive_id}) Uploading files to S3")
            upload_time_start = time.time()
            s3.upload_folder(downloads_path, f"{current_timestamp}/{drive_id}")
            logger.info(f"({drive_id}) Files uploaded in {time.time() - upload_time_start:.2f}s")

        logger.info(f"({drive_id}) Drive processed in {time.time() - start_time:.2f}s ({drive.get_file_list_length()} files)")
        current_task = "DONE"
    except Exception as e:
        logger.error(f"({drive_id}) Error processing drive: {e}")
        with open(f"{downloads_path}/errors.txt", "a") as f:
            f.write(f"Error processing drive: {e}\n")
    finally:
        stop_event.set()
        status_thread.join()


def main():


    admin_credentials = get_credentials(SETTINGS.DELEGATED_ADMIN_EMAIL)
    gadmin = GAdmin(SETTINGS.WORKSPACE_CUSTOMER_ID, admin_credentials)

    users = [user["primaryEmail"] for user in gadmin.get_user_list()]
    logger.debug(f"Users found: {users}")
    shared_drives = [drive["id"] for drive in gadmin.get_shared_drives()]
    logger.debug(f"Shared drives found: {shared_drives}")

    drives = []
    for drive_name in users:
        drives.append(GDrive(drive_name, get_credentials(drive_name), "user"))
    for drive_name in shared_drives:
        drives.append(GDrive(drive_name, admin_credentials, "shared"))

    logger.debug(f"Whiltelist: {SETTINGS.DRIVE_WHITELIST}")
    logger.debug(f"Drives initialized: {drives}")

    if len(SETTINGS.DRIVE_WHITELIST) == 0:
        logger.warning("No whitelist specified, processing all drives")
    else:
        drives = [drive for drive in drives if drive.get_drive_id() in SETTINGS.DRIVE_WHITELIST]

    logger.info(f"Drives to process: {drives}")

    random.shuffle(drives) # In case of failure, every backup will have some unique data
    
    with Pool(processes=SETTINGS.MAX_DRIVE_PROCESSES) as pool:
        current_timestamp = time.strftime("%Y%m%d-%H%M%S")
        logger.debug(f"Current timestamp: {current_timestamp}")

        args = [(drive, current_timestamp) for drive in drives]
        for _ in pool.imap_unordered(process_drive, args):
            pass


if __name__ == "__main__":
    main()