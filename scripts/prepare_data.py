import csv
import sys, os

rpath = os.path.abspath("..")
if rpath not in sys.path:
    sys.path.insert(0, rpath)

input_file = './data/traffic_data.csv'
output_file = './data/final_traffic_data.csv'

with open(input_file, 'r') as csv_input, open(output_file, 'w', newline='') as csv_output:
    reader = csv.reader(csv_input, delimiter=';')
    writer = csv.writer(csv_output)

    header = next(reader)
    writer.writerow(header)

    for row in reader:
        track_info = row[:4]

        if len(row) > 4:
            remaining_columns = row[4:]

            for i in range(0, len(remaining_columns), 6):
                position = remaining_columns[i:i + 6]
                writer.writerow(track_info + position)