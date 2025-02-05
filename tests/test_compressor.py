import os
import shutil
import tempfile
import unittest
from src.utils.compressor import Compressor


class TestCompressor(unittest.TestCase):
    def setUp(self):
        self.test_dir = tempfile.mkdtemp()
        self.files_dir = os.path.join(self.test_dir, "files")
        os.mkdir(self.files_dir)
        self.file_path = os.path.join(self.files_dir, "test_file.txt")
        with open(self.file_path, "w") as f:
            f.write("This is a test file.\n" + "Random content " * 1000)

    def tearDown(self):
        shutil.rmtree(self.test_dir)

    def test_compress_folder_lz4(self):
        compressor = Compressor(algorithm="lz4")
        tar_path, tar_size = compressor.compress_folder(self.files_dir)
        self.assertTrue(os.path.exists(tar_path))
        self.assertGreater(tar_size, 0)
        original_size = os.path.getsize(self.file_path)
        self.assertLess(tar_size, original_size)
        os.remove(tar_path)

    def test_compress_folder_pigz(self):
        compressor = Compressor(algorithm="pigz", max_processes=2)
        tar_path, tar_size = compressor.compress_folder(self.files_dir)
        self.assertTrue(os.path.exists(tar_path))
        self.assertGreater(tar_size, 0)
        original_size = os.path.getsize(self.file_path)
        self.assertLess(tar_size, original_size)
        os.remove(tar_path)

    def test_compress_folder_pzstd(self):
        compressor = Compressor(algorithm="pzstd", max_processes=2)
        tar_path, tar_size = compressor.compress_folder(self.files_dir)
        self.assertTrue(os.path.exists(tar_path))
        self.assertGreater(tar_size, 0)
        original_size = os.path.getsize(self.file_path)
        self.assertLess(tar_size, original_size)
        os.remove(tar_path)

    def test_compress_folder_delete_original(self):
        compressor = Compressor(algorithm="lz4")
        tar_path, tar_size = compressor.compress_folder(
            self.files_dir, delete_original=True
        )
        self.assertTrue(os.path.exists(tar_path))
        self.assertFalse(os.path.exists(self.files_dir))
        os.remove(tar_path)


if __name__ == "__main__":
    unittest.main()
