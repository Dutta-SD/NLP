"""
Extracting data for LARA
@author - Sandip Dutta
"""
from zipfile import ZipFile
from requests import get, head
from os import path, mkdir
from tqdm import tqdm
from ._local_config import _STORAGE_DIR, _ZIP_URL, _ZIP_FILE_NAME

class LaraDataFetcher:
    """
    Gets LARA Data from url.
    """

    def __init__(
        self,
        zip_name: str = _ZIP_FILE_NAME,
        lara_data_url: str = _ZIP_URL,
        storage_dir_path: str = _STORAGE_DIR,
        download: bool = False,
    ):

        """
        Object that handles getting LARA data object from url

        Args:
          zip_name : (str) file name of compressed data

          lara_data_url : (str) Data url of LARA files

          storage_dir_path : (str) Path to Data URL

          download : (bool) Should download the file or not.
            Defaults to False. If using for the first time,
            set it to True

        Returns:
          LaraDataFetcher object

        Raises:
          FileNotFoundError : If not downloaded data or file is 
            missing
        """
        self._lara_data_url = lara_data_url
        self._storage_dir_path = storage_dir_path
        self._download = download
        self._zip_name = zip_name

        # Download progress parameter
        self._chunk_size = 128

    def __call__(self):
        """Downloads and Extracts the LARA Data"""

        if self._download:
            self.download_data()

        if not path.exists(self._storage_dir_path):
            mkdir(self._storage_dir_path)

        with ZipFile(self._zip_name) as data_file:
            data_file.extractall(self._storage_dir_path)

        print(f"Extraction Complete at : {self._storage_dir_path}")

    def download_data(self):
        """Downloads the data as chunks and displays progress bar"""

        # https://github.com/sirbowen78/lab/blob/master/file_handling/dl_file1.py
        filesize = int(head(self._lara_data_url).headers["Content-Length"])

        with get(self._lara_data_url, stream=True) as r, open(
            self._zip_name, "wb"
        ) as f, tqdm(
            unit="B",  # unit string to be displayed.
            unit_scale=True,  # let tqdm to determine the scale in kilo, mega..etc.
            unit_divisor=1024,  # is used when unit_scale is true
            total=filesize,  # the total iteration.
            desc=self._zip_name,  # prefix to be displayed on progress bar.
        ) as progress:
            for chunk in r.iter_content(chunk_size=self._chunk_size):
                # download the file chunk by chunk
                datasize = f.write(chunk)
                # on each chunk update the progress bar.
                progress.update(datasize)
