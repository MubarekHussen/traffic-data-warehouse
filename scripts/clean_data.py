import pandas as pd

def read_csv_to_df(file_path):
    return pd.read_csv(file_path)

def remove_duplicates(df, column_name, file_path):
    df.drop_duplicates(subset=column_name, keep='first', inplace=True)
    df.to_csv(file_path, index=False)  # Overwrite the original file
    print("Duplicates removed and file overwritten successfully.")

def clean_data(df, columns, file_path):
    # Convert columns to appropriate data types and handle errors
    for col in columns:
        df[col] = pd.to_numeric(df[col], errors='coerce')

    # Remove rows with NaN values
    df = df.dropna()

    df.to_csv(file_path, index=False)  # Overwrite the original file
    print("Data cleaned and file overwritten successfully.")

# File with duplicates
file_path_duplicates = './data/trajectory_data.csv'
df_duplicates = read_csv_to_df(file_path_duplicates)
remove_duplicates(df_duplicates, 'track_id', file_path_duplicates)

# File with NaN values
file_path_nan = './data/vehicle_positions_data.csv'
df_nan = read_csv_to_df(file_path_nan)
clean_data(df_nan, [' lat', ' lon', ' speed', ' lon_acc', ' lat_acc', ' time'], file_path_nan)