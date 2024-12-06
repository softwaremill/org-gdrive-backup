from loguru import logger
import os
import time
import shutil

class Compressor:
    def __init__(self, algorithm, delete_original=False, max_processes=1):
        self.algorithm = algorithm
        self.delete_original = delete_original
        self.max_processes = max_processes

    def compress_folder(self, path):
        if not os.path.isdir(path):
            raise ValueError(f"{path} is not a directory")
        logger.info(f"Compressing {path} with {self.algorithm}")
        start_time = time.time()
        
        path_parts = path.split("/")
        file_name = path_parts[-1]
        directory = "/".join(path_parts[:-1])

        match self.algorithm:
            case "lz4":
                tar_path = f"{directory}/{file_name}.tar.lz4"
                success = os.system(f"tar c - -C {directory} {file_name} | lz4 - {tar_path}")
            case "pigz":
                tar_path = f"{directory}/{file_name}.tar.gz"
                success = os.system(f"tar cf - -C {directory} {file_name} | pigz -p {self.max_processes} > {tar_path}")
            case _:
                raise NotImplementedError(f"Compression algorithm {self.algorithm} not implemented")
            
        if self.delete_original:
            shutil.rmtree(path)

        logger.info(f"Compressing {path} with {self.algorithm} took {time.time() - start_time:.2f}s")
