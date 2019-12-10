import csv
import sys

import re

data_dir = '../task2data/'
source = data_dir + 'columns_labeled.csv'
semantic_data_dir = '../semantic_data/'

def read_column_values(values, column_file_name):
    with open(data_dir + column_file_name, newline='') as csvfile:
        reader = csv.reader(csvfile, delimiter='\t')
        for row in reader:
            if not re.search('[a-zA-Z\d]', row[0]):
                continue
            if int(row[1]) < 20:
                continue
            values.append(row[0])

if __name__ == "__main__":
    num_columns = -1
    num_correct = 0
    values = []
    with open(source, newline='') as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            num_columns += 1
            if num_columns == 0:
                continue

            column_file_name = row[0]
            labels = []
            searched = "Vehicle Type"
            row = row[1:]
            if searched in row:
                num_correct += 1
                read_column_values(values, column_file_name)

    print(num_correct)
    values = sorted((set(values)))
    with open(semantic_data_dir + "VehicleTypes.txt", "w+") as f:
        for val in values:
            f.write("%s\n"%val)