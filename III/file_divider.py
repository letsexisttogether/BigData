import pandas as pd
import os

from names import DATASETS, VOTES_DATASETS


def divide_file(df, num_files, output_dir):
    total_rows = len(df)
    rows_per_file = total_rows // num_files

    for i in range(num_files):
        start_index = i * rows_per_file
        if i == num_files - 1:  # Last file gets the remaining rows
            end_index = total_rows
        else:
            end_index = start_index + rows_per_file

        partition_df = df.iloc[start_index:end_index]

        file_path = os.path.join(output_dir, f"file_{i + 1}.csv")
        partition_df.to_csv(file_path, index=False)


def execute():
    existing_df = pd.read_csv(DATASETS + 'unvotes.csv')

    divide_file(existing_df, 3, VOTES_DATASETS)
