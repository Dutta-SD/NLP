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
        self.data_frame = None
        # Columns set or not
        self._is_col_set = False
        self._col_names = None

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

    def check_cols(self):
        if len(self._col_names) != self._n_data_rows:
            raise AttributeError(
                f"Expected {self._n_data_rows} cols, got {len(self._col_names)}"
            )

    def dat_to_df(self, curr_file):

        # Temporary colums for parsing. Some files causes errors
        temp_cols = [0, 1]

        df = pd.read_csv(curr_file, sep=self._sep, header=None, names=temp_cols)

        actual_data = df.iloc[
            self._n_meta_rows :,
        ]

        if not self._is_col_set:
            self._col_names = [
                col[1:] for col in actual_data.iloc[: self._n_data_rows, 0]
            ]
            self.check_cols()
            self._is_col_set = True

        i = 0

        data_remade = pd.DataFrame(columns=self._col_names)

        while i < len(actual_data):

            try:

                temp = actual_data.iloc[i : i + self._n_data_rows, 1].values
                temp.resize((1, self._n_data_rows))
                df_temp = pd.DataFrame(temp, columns=self._col_names)

                data_remade = pd.concat(
                    [data_remade, df_temp], axis=0
                )

                i += self._n_data_rows

            except ValueError:
                i += self._n_data_rows

        if self.data_frame is None:
            self.check_cols()
            self.data_frame = pd.DataFrame(columns=self._col_names)
            print(self._col_names)
            print(self.data_frame.columns)

        self.data_frame = pd.concat([self.data_frame, data_remade], axis=0)
        print(f"SHAPE AFTER APPEND : {self.data_frame.shape}")

    def save_as_csv(self):
        self.data_frame.to_csv(f"./{self._fileName}.csv", index=False)
