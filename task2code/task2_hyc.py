import re
import csv

def predict(column_file_name, values):
    pass

def read_column_values(column_file_name):
    values = []
    with open(data_dir + column_file_name, newline='') as csvfile:
        reader = csv.reader(csvfile, delimiter='\t')
        for row in reader:
            values.append((row[0], row[1]))
    return values

data_dir = '../task2data/'
source = data_dir + 'columns_labeled.csv'
if __name__ == "__main__":
    num_columns = -1
    num_correct = 0
    with open(source, newline='') as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            num_columns += 1
            if num_columns == 0:
                continue

            ### TEST ONLY ###
            # if num_columns > 50:
            #     break
            #################

            # Parse file name and labels
            column_file_name = row[0]
            labels = []
            for i in range(1, len(row)):
                if not row[i]:
                    break
                labels.append(row[i])
            print("File name: %s"%column_file_name)
            print("Labels: %s"%labels)

            values = read_column_values(column_file_name)

            predict(column_file_name, values)

    # Calculate correct rate
    print(num_columns)