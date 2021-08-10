"""
Converts the data into a csv file.
@author - Sandip Dutta
"""
import pandas as pd
from ._local_config import _STORAGE_DIR
import os

class LARAToDataFile:
    """
    Converts LARA data procured as .dat file that can be consumed easily.
    """
    def __init__(self, final_fileName : str, n_meta_rows : int = 3, n_data_rows : int = 13, sep = ">"):
        self._n_meta_rows = n_meta_rows
        self._n_data_rows = n_data_rows
        self._sep = sep

        # Will be set later
        self._fileName = final_fileName
        self.data_frame = pd.DataFrame()

    def _check_dir_exists(self):
        if not os.path.exists(_STORAGE_DIR):
            raise FileNotFoundError(
                f"No Directory found at {_STORAGE_DIR}"
            )
        if len(os.listdir(_STORAGE_DIR)) == 0:
            raise OSError(
                f"0 files found at {_STORAGE_DIR}"
            )

    def __call__(self, fmt = 'csv'):
        self._check_dir_exists()
        if fmt == 'csv':
            for curr_file in os.listdir(_STORAGE_DIR):
                self.dat_to_csv(curr_file)
        else:
            raise NotImplementedError(
                "Sorry. CSV only!!!"
            )
    
    def dat_to_csv(self, curr_file):
        filename = os.path.join(_STORAGE_DIR, curr_file)
        df = pd.read_csv(filename, sep = self._sep, header = None)

        # meta_data = df.iloc[ : self._n_meta_rows, : ]
        actual_data = df.iloc[ self._n_meta_rows: , ]

        col_names = [col[1:] for col in actual_data.iloc[ : , 0].tolist()]

        i = 0

        data_remade = pd.DataFrame()

        while i < len(actual_data):
            df_temp = pd.DataFrame(actual_data.iloc[i : i + self._n_data_rows, 1].values.reshape(-1, 1))
            data_remade = pd.concat([data_remade, df_temp], ignore_index=True, axis = 1)
            i += self._n_data_rows

        data_remade = data_remade.T

        self.data_frame = pd.concat([self.data_frame, data_remade], ignore_index=True, axis = 1)
        self.data_frame.columns = col_names

    def save_as_csv(self):
        pd.to_csv(f"./{self._fileName}.{self.csv}", index = False)