import pandas as pd

# read the CSV files into pandas DataFrames
df_trajectory = pd.read_csv('./data/trajectory_data.csv')
df_vehicle_positions = pd.read_csv('./data/vehicle_positions_data.csv')
print(df_trajectory.head())
print()
print(df_vehicle_positions.head())