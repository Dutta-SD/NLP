"""
Converts the data into a csv file.
@author - Sandip Dutta
"""
import pandas as pd
from ._local_config import _STORAGE_DIR
import os
import glob


class LARAToDataFile:
    """
    Converts LARA data procured as .dat file that can be consumed easily.
    """

    def __init__(
        self, final_fileName: str, n_meta_rows: int = 3, n_data_rows: int = 13, sep=">"
    ):
        self._n_meta_rows = n_meta_rows
        self._n_data_rows = n_data_rows
        self._sep = sep

        # Will be set later
        self._fileName = final_fileName
        self.data_frame = pd.DataFrame()
        # Columns set or not
        self._is_col_set = False

    def _check_dir_exists(self):
        if not os.path.exists(_STORAGE_DIR):
            raise FileNotFoundError(f"No Directory found at {_STORAGE_DIR}")
        if len(glob.glob(f"{_STORAGE_DIR}/*.dat")) == 0:
            raise OSError(f"0 files found at {_STORAGE_DIR}")

    def __call__(self, fmt="csv"):
        self._check_dir_exists()
        if fmt == "csv":
            for curr_file in glob.glob(f"{_STORAGE_DIR}/*.dat"):
                print(curr_file)
                self.dat_to_df(curr_file)
        else:
            raise NotImplementedError("Sorry. CSV only!!!")

    def dat_to_df(self, curr_file):
        df = pd.read_csv(curr_file, sep=self._sep, header=None)

        # meta_data = df.iloc[ : self._n_meta_rows, : ]
        actual_data = df.iloc[
            self._n_meta_rows :,
        ]

        col_names = [
            col[1:] for col in actual_data.iloc[: self._n_data_rows, 0].tolist()
        ]
        # print(col_names)

        i = 0

        data_remade = pd.DataFrame()

        while i < len(actual_data):

            try:

                temp = actual_data.iloc[i : i + self._n_data_rows, 1].values
                temp.resize((1, self._n_data_rows))
                df_temp = pd.DataFrame(temp)
        
                data_remade = pd.concat([data_remade, df_temp], ignore_index=True, axis=0)

                i += self._n_data_rows
            
            except ValueError:
                i += self._n_data_rows


        self.data_frame = self.data_frame.append(data_remade, ignore_index=True)

        if not self._is_col_set:
            self._is_col_set = True
            self.data_frame.columns = col_names

    def save_as_csv(self):
        self.data_frame.to_csv(f"./{self._fileName}.csv", index=False)
