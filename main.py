import os.path
import random
import shutil
import time
import threading
from multiprocessing import Pool
from google.oauth2.service_account import Credentials
from typing import Tuple

from src.google.gadmin import GAdmin
from src.google.gdrive import GDrive, DRIVE_TYPE
from src.aws.s3 import S3
from src.utils.compressor import Compressor
from src.utils.logger import app_logger as logger
from src.utils.settings import Settings
from src.enums import STATE


SETTINGS = Settings()
SCOPES = [
    "https://www.googleapis.com/auth/admin.directory.user.readonly",
    "https://www.googleapis.com/auth/drive.readonly",
]

random.seed(time.time())


def get_credentials(subject: str) -> Credentials:
    return Credentials.from_service_account_file(
        SETTINGS.SERVICE_ACCOUNT_FILE, scopes=SCOPES
    ).with_subject(subject)


def download_files_from_drive(
    drive: GDrive, metadata_path: str, files_path: str
) -> None:
    drive_id = drive.drive_id
    drive.fetch_file_list()
    logger.debug(f"({drive_id}) Files found: {len(drive.files)}")
    drive.dump_file_list(metadata_path)
    logger.info(f"({drive_id}) File list saved to {metadata_path}")

    logger.info(f"({drive_id}) Downloading {len(drive.files)} files")
    drive.download_all_files(files_path, threads=SETTINGS.MAX_DOWNLOAD_THREADS)
    logger.info(f"({drive_id}) Files downloaded")


def compress_files_from_drive(drive_id: str, files_path: str) -> None:
    logger.info(f"({drive_id}) Compressing files")
    compress_time_start = time.time()
    compressor = Compressor(
        SETTINGS.COMPRESSION_ALGORITHM, max_processes=SETTINGS.COMPRESSION_PROCESSES
    )
    _, tar_size = compressor.compress_folder(files_path, delete_original=True)
    logger.info(
        f"({drive_id}) Files compressed in {time.time() - compress_time_start:.2f}s ({tar_size/1024/1024:.2f}MB)"
    )


def drive_cleanup(drive_id: str, downloads_path: str) -> None:
    logger.info(f"({drive_id}) Cleaning up")
    try:
        shutil.rmtree(downloads_path)
        logger.info(f"({drive_id}) Cleanup complete")
    except Exception as e:
        logger.error(f"({drive_id}) Error cleaning up: {e}")


def upload_files_to_s3(drive_id: str, downloads_path: str, timestamp: str) -> None:
    if SETTINGS.S3_ROLE_BASED_ACCESS:
        s3 = S3(SETTINGS.S3_BUCKET_NAME, None, None, role_based=True)
    else:
        s3 = S3(SETTINGS.S3_BUCKET_NAME, SETTINGS.S3_ACCESS_KEY, SETTINGS.S3_SECRET_KEY)
    logger.info(f"({drive_id}) Uploading files to S3")
    upload_time_start = time.time()
    upload_size = s3.upload_folder(downloads_path, f"{timestamp}/{drive_id}")
    upload_size_mb = upload_size / 1024 / 1024
    upload_speed_mb = upload_size_mb / (time.time() - upload_time_start)
    logger.info(
        f"({drive_id}) Files uploaded in {time.time() - upload_time_start:.2f}s ({upload_size_mb:.2f}MB, {upload_speed_mb:.2f}MB/s)"
    )


def process_drive(args: Tuple[GDrive, str]) -> bool:
    current_task = STATE.STARTING
    drive, current_timestamp = args
    start_time = time.time()
    drive_id = drive.drive_id
    downloads_path = f"downloads/{current_timestamp}/{drive_id}"
    metadata_path = f"{downloads_path}/files.json"
    files_path = f"{downloads_path}/files"

    stop_event = threading.Event()
    status_thread = None

    def print_status():
        counter = 0
        while not stop_event.is_set():
            time.sleep(1)
            counter += 1
            if counter % 60 == 0 and current_task != STATE.DONE:
                logger.info(
                    f"({drive_id}) Current status: {current_task.value}. Files found: {len(drive.files)}. Time elapsed: {time.time() - start_time:.2f}s"
                )

    try:
        status_thread = threading.Thread(target=print_status, daemon=True)
        status_thread.start()

        logger.info(f"({drive_id}) Started processing drive")

        if SETTINGS.JIT_S3_UPLOAD:
            current_task = STATE.DOWNLOADING_AND_JIT_UPLOADING
        else:
            current_task = STATE.DOWNLOADING
        download_files_from_drive(drive, metadata_path, files_path)

        file_count = len(drive.files)

        if SETTINGS.COMPRESS_DRIVES and file_count > 0:
            current_task = STATE.COMPRESSING
            compress_files_from_drive(drive_id, files_path)
        elif SETTINGS.COMPRESS_DRIVES and file_count == 0:
            logger.debug(f"({drive_id}) No files found, skipping compression")
        else:
            logger.debug(f"({drive_id}) Compression disabled")

        if file_count > 0:
            current_task = STATE.UPLOADING
            upload_files_to_s3(drive_id, downloads_path, current_timestamp)
        else:
            logger.warning(f"({drive_id}) No files found, skipping upload")

        current_task = STATE.DONE
        logger.info(
            f"({drive_id}) Drive processed in {time.time() - start_time:.2f}s - ({len(drive.files)} files)"
        )
        return True

    except Exception as e:
        logger.error(f"({drive_id}) Error processing drive: {e}")
        os.makedirs(f"{downloads_path}", exist_ok=True)
        with open(f"{downloads_path}/errors.txt", "a") as f:
            f.write(f"Error processing drive: {e}\n")
        return False
    finally:
        stop_event.set()
        if status_thread and status_thread.is_alive():
            status_thread.join(timeout=1.0)


def main():
    start_time = time.time()
    admin_credentials = get_credentials(SETTINGS.DELEGATED_ADMIN_EMAIL)
    gadmin = GAdmin(SETTINGS.WORKSPACE_CUSTOMER_ID, admin_credentials)

    users = [user["primaryEmail"] for user in gadmin.get_user_list()]
    logger.debug(f"Users found: {users}")
    shared_drives = [drive["id"] for drive in gadmin.get_shared_drives()]
    logger.debug(f"Shared drives found: {shared_drives}")

    drives = []
    for drive_name in users:
        drives.append(
            GDrive(
                drive_name,
                get_credentials(drive_name),
                DRIVE_TYPE.USER,
                SETTINGS.JIT_S3_UPLOAD,
                SETTINGS.S3_ROLE_BASED_ACCESS,
                SETTINGS.S3_BUCKET_NAME,
                SETTINGS.S3_ACCESS_KEY,
                SETTINGS.S3_SECRET_KEY,
            )
        )
    for drive_name in shared_drives:
        drives.append(
            GDrive(
                drive_name,
                admin_credentials,
                DRIVE_TYPE.SHARED,
                SETTINGS.JIT_S3_UPLOAD,
                SETTINGS.S3_ROLE_BASED_ACCESS,
                SETTINGS.S3_BUCKET_NAME,
                SETTINGS.S3_ACCESS_KEY,
                SETTINGS.S3_SECRET_KEY,
            )
        )

    logger.debug(f"Drives initialized: {drives}")
    logger.info(f"Whitelist: {SETTINGS.DRIVE_WHITELIST}")
    logger.info(f"Blacklist: {SETTINGS.DRIVE_BLACKLIST}")

    if len(SETTINGS.DRIVE_WHITELIST) == 0:
        logger.warning("No whitelist specified, processing all drives")
    else:
        drives = [
            drive for drive in drives if drive.drive_id in SETTINGS.DRIVE_WHITELIST
        ]

    if len(SETTINGS.DRIVE_BLACKLIST) > 0:
        drives = [
            drive for drive in drives if drive.drive_id not in SETTINGS.DRIVE_BLACKLIST
        ]

    logger.info(f"Drives to process: {drives}")

    random.shuffle(
        drives
    )  # In case of failure, every backup will have some unique data

    with Pool(processes=SETTINGS.MAX_DRIVE_PROCESSES) as pool:
        current_timestamp = time.strftime("%Y%m%d-%H%M%S")
        logger.debug(f"Current timestamp: {current_timestamp}")

        remaining_drives = [(drive, current_timestamp) for drive in drives]
        processed_drives = set()
        failed_drives = set()
        running_processes = []

        # Initial process spawning
        while (
            len(running_processes) < SETTINGS.MAX_DRIVE_PROCESSES and remaining_drives
        ):
            drive_args = remaining_drives.pop(0)
            result = pool.apply_async(process_drive, (drive_args,))
            running_processes.append((drive_args, result))
            logger.info(f"Started processing drive {drive_args[0].drive_id}")

        # Main processing loop
        while running_processes or remaining_drives:
            # Check for completed processes
            for drive_args, result in running_processes[:]:
                if result.ready():
                    drive = drive_args[0]
                    success = result.get()
                    if success:
                        processed_drives.add(drive.drive_id)
                    else:
                        failed_drives.add(drive.drive_id)

                    running_processes.remove((drive_args, result))

                    # Spawn new process if there are remaining drives
                    if remaining_drives:
                        new_drive_args = remaining_drives.pop(0)
                        new_result = pool.apply_async(process_drive, (new_drive_args,))
                        running_processes.append((new_drive_args, new_result))

            time.sleep(1)  # Short sleep to prevent CPU spinning

        total_time = time.time() - start_time
        logger.info(f"Backup completed in {total_time:.2f}s")
        logger.info(
            f"Successfully processed drives: {len(processed_drives)}/{len(drives)}"
        )
        logger.debug(f"Successfully processed drives: {processed_drives}")
        if len(processed_drives) < len(drives):
            logger.warning("Some drives were not processed successfully!")
            logger.warning(f"Failed drives: {failed_drives}")


if __name__ == "__main__":
    main()
