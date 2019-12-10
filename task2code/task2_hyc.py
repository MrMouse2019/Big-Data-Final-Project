import re
import csv

data_dir = '../task2data/'
source = data_dir + 'columns_labeled.csv'
semantic_data_dir = '../semantic_data/'
# semantic_set_list = ['Color', 'VehicleType', 'BusinessName']
semantic_set_list = ['Color', 'VehicleType', 'BusinessName']
semantic_set = []

def is_biz_name(val, col_fname):
    val = val.lower()
    freq_list = ['inc', 'llc', 'corp', 'corporation', 'service']
    global semantic_set
    words = val.split()
    num_words = len(val)
    num_matched = 0

    for word in freq_list:
        if word in val and num_words > 1:
            return True
    for items in semantic_set:
        for word in words:
            if word in items:
                num_matched += 1
        if num_matched >= 2 or num_matched >= 0.5 * num_words:
            return True
    return False

def is_color(val, col_fname):
    global semantic_set
    if not 'colo' in col_fname and len(val) < 3:
        return False
    if val in semantic_set:
        return True
    return False

def is_vehicle_type(val, col_fname):
    global semantic_set
    if not ('body' in col_fname and 'type' in col_fname) and len(val) < 2:
        return False
    if 'make' in col_fname:
        return False
    if val in semantic_set:
        return True
    return False

def do_nothing(val, col_fname):
    return False

def is_phone_num(val):
    if re.fullmatch("^\(?\d{3}\)?-? *\d{3}-? *-?\d{4}$", val):
        return True
    return False

def is_coord(val):
    if re.match("^\(-?\d+\.\d+, *-?\d+\.\d+\)$", val):
        return True
    return False

def is_zipcode(val):
    strs = val.split()
    str = ''.join(strs)
    if re.fullmatch("^[0-9]{5}(?:-[0-9]{4})?$", str):
        return True
    return False

def is_website(val):
    str = val.lower()
    if not re.search('[a-zA-Z]', str):
        return False
    if re.search('(\. +)|( +\.)', str):
        return False
    if re.search('[,\[\]]', str):
        return False
    if re.search("((https?|ftp|smtp):\/\/)?(www.)?[a-z0-9]+(\-[a-z0-9]+)*(\.[a-z0-9]+(\-[a-z0-9]+)*)+(\/|(\/[a-z0-9#]+\/?))*", str):
        return True
    return False

def label_cell_regex(val):
    # For regex predicting semantics.
    if is_zipcode(val):
        return 'ZipCode'
    if is_website(val):
        return 'Website'
    if is_phone_num(val):
        return 'PhoneNumber'
    if is_coord(val):
        return 'Coordinate'
    return

def load_set(semantic):
    global semantic_set
    f_name = semantic_data_dir + semantic + 's.txt'
    with open(f_name) as f:
        semantic_set = f.readlines()
    if semantic == 'BusinessName':
        semantic_set = [x.strip().lower().split() for x in semantic_set]
    else:
        semantic_set = [x.strip() for x in semantic_set]

def filter_classified(values):
    values_ = []
    for val in values:
        if val:
            values_.append(val)
    return values_

def predict(column_file_name, values):
    thre = 0.05
    pred_labels_temp = {}
    num_items = 0

    for i in range(len(values)):
        val = values[i]
        cell_label = label_cell_regex(val[0])
        cell_freq = int(val[1])
        num_items += cell_freq
        if not cell_label:
            continue
        if cell_label in pred_labels_temp:
            pred_labels_temp[cell_label] += cell_freq
        else:
            pred_labels_temp[cell_label] = cell_freq
        values[i] = None

    # Filter out all classified items
    values = filter_classified(values)

    # Classify items by knowledge.
    global semantic_set
    for semantic in semantic_set_list:
        # print("Predicting semantic: %s.."%semantic)
        load_set(semantic)
        func = None
        if semantic == 'BusinessName':
            func = is_biz_name
        elif semantic == 'Color':
            func = is_color
        elif semantic == 'VehicleType':
            func = is_vehicle_type
        else:
            func = do_nothing

        for i in range(len(values)):
            val = values[i]
            cell_freq = int(val[1])
            if func(val[0], column_file_name):
                values[i] = None
                if semantic in pred_labels_temp:
                    pred_labels_temp[semantic] += cell_freq
                else:
                    pred_labels_temp[semantic] = cell_freq
                values[i] = None
        values = filter_classified(values)
        semantic_set = []

    pred_labels = {}
    for key in pred_labels_temp:
        match_rate = pred_labels_temp[key] / num_items
        if match_rate >= thre:
            pred_labels[key] = pred_labels_temp[key]
    print(pred_labels)
    return

def read_column_values(column_file_name):
    values = []
    with open(data_dir + column_file_name, newline='') as csvfile:
        reader = csv.reader(csvfile, delimiter='\t')
        for row in reader:
            values.append((row[0], row[1]))
    return values

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
            column_file_name = column_file_name.lower()

            predict(column_file_name, values)

    # Calculate correct rate
    print(num_columns)