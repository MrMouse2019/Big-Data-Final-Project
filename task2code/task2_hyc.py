import re
import csv

def is_phone_num(values):
    thre = 0.8
    num_match = 0
    for val in values:
        if re.fullmatch("^\(?\d{3}\)?-? *\d{3}-? *-?\d{4}$", val[0]):
            num_match += 1
    match_rate = num_match / len(values)
    if match_rate > thre:
        return True
    return False

def is_coord(values):
    thre = 0.9
    num_match = 0
    for val in values:
        if re.match("^\(-?\d+\.\d+, *-?\d+\.\d+\)$", val[0]):
            num_match += 1
    match_rate = num_match / len(values)
    if match_rate > thre:
        return True
    return False

def is_zipcode(values):
    thre = 0.5
    num_match = 0
    for val in values:
        strs = val[0].split()
        str = ''.join(strs)
        if re.fullmatch("^[0-9]{5}(?:-[0-9]{4})?$", str):
            num_match += 1
    match_rate = num_match/len(values)
    if match_rate > thre:
        return True
    return False

def is_website(values):
    thre = 0.9
    num_match = 0
    for val in values:
        str = val[0].lower()
        if not re.search('[a-zA-Z]', str):
            continue
        if re.search("((https?|ftp|smtp):\/\/)?(www.)?[a-z0-9]+(\-[a-z0-9]+)*(\.[a-z0-9]+(\-[a-z0-9]+)*)+(\/|(\/[a-z0-9#]+\/?))*", str):
            num_match += 1
    match_rate = num_match / len(values)
    if match_rate > thre:
        return True
    return False

def predict(column_file_name, values):
    if is_zipcode(values):
        print('Is a zip code')
    if is_website(values):
        print('Is a website')
    if is_coord(values):
        print('Is a coordinate')
    if is_phone_num(values):
        print('Is a phone number')
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