import csv
from io import StringIO
import sys, os

rpath = os.path.abspath("..")
if rpath not in sys.path:
    sys.path.insert(0, rpath)

def split_data_to_tables(input_csv_path, output_trajectory_path, output_vehicle_positions_path):
    with open(input_csv_path, 'r') as csv_file:
        csv_reader = csv.reader(csv_file)
        header = next(csv_reader)

        trajectory_info_rows = []
        vehicle_positions_rows = []

        for row in csv_reader:
            trajectory_info_rows.append(row[:4])
            vehicle_positions_rows.append([row[0]] + row[4:])

    with open(output_trajectory_path, 'w', newline='') as trajectory_csv:
        csv_writer = csv.writer(trajectory_csv)
        csv_writer.writerow(header[:4])
        csv_writer.writerows(trajectory_info_rows)

    with open(output_vehicle_positions_path, 'w', newline='') as vehicle_positions_csv:
        csv_writer = csv.writer(vehicle_positions_csv)
        csv_writer.writerow(header[:1] + header[4:])
        csv_writer.writerows(vehicle_positions_rows)


split_data_to_tables(
    './data/final_traffic_data.csv',
    './data/trajectory_data.csv',
    './data/vehicle_positions_data.csv'
)
