import os
import shutil

class Compressor:
    def __init__(self, algorithm, delete_original=False, max_processes=1):
        self.algorithm = algorithm
        self.delete_original = delete_original
        self.max_processes = max_processes

    def compress_folder(self, path):
        if not os.path.isdir(path):
            raise ValueError(f"{path} is not a directory")
        
        path_parts = path.split("/")
        file_name = path_parts[-1]
        directory = "/".join(path_parts[:-1])

        match self.algorithm:
            case "lz4":
                tar_path = f"{directory}/{file_name}.tar.lz4"
                exit_code = os.system(f"tar c - -C {directory} {file_name} | lz4 - {tar_path}")
            case "pigz":
                tar_path = f"{directory}/{file_name}.tar.gz"
                exit_code = os.system(f"tar cf - -C {directory} {file_name} | pigz -p {self.max_processes} > {tar_path}")
            case _:
                raise NotImplementedError(f"Compression algorithm {self.algorithm} not implemented")
            
        tar_size = os.path.getsize(tar_path)
        
        if exit_code != 0:
            raise RuntimeError(f"Compression failed with exit code {exit_code}")
            
        if self.delete_original:
            shutil.rmtree(path)

        return tar_path, tar_size