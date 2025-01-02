import os
import boto3
from src.utils.logger import app_logger as logger
from src.enums import STORAGE_CLASS

class S3:
    def __init__(self, bucket_name, access_key, secret_key):
        self.bucket_name = bucket_name
        self.s3 = boto3.client("s3", aws_access_key_id=access_key, aws_secret_access_key=secret_key)

    def upload_folder(self, source_path, destination_path, storage_class: STORAGE_CLASS = STORAGE_CLASS.STANDARD):
        if not os.path.isdir(source_path):
            raise ValueError(f"{source_path} is not a directory")
        
        file_size_counter = 0
        
        for root, _, files in os.walk(source_path):
            for file in files:
                try:
                    file_size_counter += os.path.getsize(os.path.join(root, file))
                    file_path = os.path.join(root, file)
                    key = f"{destination_path}{file_path.replace(source_path, '')}"
                    self.s3.upload_file(file_path, self.bucket_name, key, ExtraArgs={"StorageClass": storage_class.value})
                    logger.trace(f"Uploaded {file_path} to {key}")
                except Exception as e:
                    logger.error(f"Error uploading {file_path} to {key}: {e}")
                    raise e
        
        return file_size_counter

    def upload_file(self, source_path, destination_path, storage_class: STORAGE_CLASS = STORAGE_CLASS.STANDARD):
        if not os.path.isfile(source_path):
            raise ValueError(f"{source_path} is not a file")
        try:
            self.s3.upload_file(source_path, self.bucket_name, destination_path, ExtraArgs={"StorageClass": storage_class.value})
            logger.trace(f"Uploaded {source_path} to {destination_path}")
        except Exception as e:
            logger.error(f"Error uploading {source_path} to {destination_path}: {e}")
            raise e

        

        
    
        
