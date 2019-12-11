import re
import csv
import editdistance
import json

data_dir = '../task2data/'
source = data_dir + 'columns_labeled.csv'
semantic_data_dir = '../semantic_data/'
# semantic_set_list =
# ['Subject', 'AreasStudy', 'CityAgency', 'Color', 'VehicleType', 'Borough', 'Neighborhood', 'City', 'StreetName', 'BusinessName']
semantic_set_list = ['BuildingCls', 'Subject', 'AreasStudy', 'SchoolLevel', 'CityAgency', 'Color'
    , 'TypeLocation', 'VehicleType', 'CarMake', 'Borough', 'Neighborhood', 'City', 'School', 'Address', 'Park'
    , 'Street', 'CollegeName', 'PersonName', 'BusinessName']
semantic_set = []
addr_identifiers = ['street', 'st', 'avenue', 'ave', 'boulevard', 'blvd', 'place', 'pl', 'road']
freq_list = ['inc', 'llc', 'corp', 'corporation', 'service']
label_dict = {
    'BuildingCls': 'building_classification',
    'Subject': 'subject_in_school',
    'AreasStudy': 'area_of_study',
    'SchoolLevel': 'school_level',
    'CityAgency': 'city_agency',
    'Color': 'color',
    'TypeLocation': 'location_type',
    'VehicleType': 'vehicle_type',
    'CarMake': 'car_make',
    'Borough': 'borough',
    'Neighborhood': 'neighborhood',
    'City': 'city',
    'School': 'school_name',
    'Address': 'address',
    'Park': 'park_playground',
    'Street': 'street_name',
    'CollegeName': 'college_name',
    'PersonName': 'person_name',
    'BusinessName': 'business_name',
    'ZipCode': 'zip_code',
    'Website': 'website',
    'PhoneNumber': 'phone_number',
    'Coordinate': 'lat_lon_cord'
}

def is_biz_name(val, col_fname):
    global freq_list
    global semantic_set
    words = val.split()
    num_words = len(val)
    num_matched = 0

    if not 'dba' in col_fname and len(val) < 3:
        return False
    for word in freq_list:
        if word in words and num_words > 1:
            return True
    for items in semantic_set:
        for word in words:
            if word in items:
                num_matched += 1
        if num_matched >= 2 or num_matched >= 0.66 * num_words:
            return True
    return False

def is_person_name(val, col_fname):
    if ('first' in col_fname or 'last' in col_fname) and 'name' in col_fname:
        return True
    if 'MI' in col_fname:
        return True
    return False

def is_univ_name(val, col_fname):
    words = val.split()
    if ('college' in words or 'university' in words) and len(words) > 1:
        return True
    return False

def is_school(val, col_fname):
    global semantic_set
    if 'park' in col_fname or 'play' in col_fname and 'closed' in val:
        return False
    if val in semantic_set:
        return True
    return False

def is_park(val, col_fname):
    global semantic_set
    words = val.split()
    for word in words:
        if word in semantic_set:
            return True
    return False

def is_addr(val, col_fname):
    if 'address' in col_fname:
        return True
    words = val.split()
    if len(words) < 3:
        return False
    try:
        int(words[0])
    except:
        return False
    words = words[3:]
    for word in words:
        if word in addr_identifiers:
            return True
    return False

def is_st(val, col_fname):
    if 'street' in col_fname:
        return True
    words = val.split()
    for word in words:
        if word in addr_identifiers:
            return True
    return False


def is_building_cls(val, col_fname):
    global semantic_set
    for cls in semantic_set:
        if val == cls:
            return True
    return False

def is_type_location(val, col_fname):
    global semantic_set
    if val == 'other' and not ('prem' in col_fname and 'typ' in col_fname):
        return False
    for type_location in semantic_set:
        if val == type_location:
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
    if 'dba' in col_fname:
        return False
    if 'make' in col_fname:
        return False
    if val in semantic_set:
        return True
    return False

def is_car_make(val, col_fname):
    if not 'make' in col_fname and len(val) < 3:
        return False
    if 'dba' in col_fname:
        return False
    if val in semantic_set:
        return True
    return False

def is_borough(val, col_fname):
    global semantic_set
    if not 'boro' in col_fname and len(val) < 2:
        return False
    if 'dba' in col_fname or 'street' in col_fname:
        return False
    words = val.split()
    if len(words) > 2:
        return False
    for borough in semantic_set:
        if editdistance.eval(val, borough) < 3:
            return True
    return False

def is_neighborhood(val, col_fname):
    global semantic_set
    if not 'nei' in col_fname and len(val) < 3:
        return False
    if ('first' in col_fname or 'last' in col_fname) and 'name' in col_fname:
        return False
    if 'dba' in col_fname:
        return False
    if 'boro' in col_fname:
        return False
    for neigh in semantic_set:
        if editdistance.eval(val, neigh) < 3:
            return True
    return False

def is_city(val, col_fname):
    global semantic_set
    if ('first' in col_fname or 'last' in col_fname) and 'name' in col_fname:
        return False
    if 'dba' in col_fname or 'street' in col_fname:
        return False
    if not 'city' in col_fname and len(val) < 3:
        return False
    for city in semantic_set:
        if editdistance.eval(val, city) < 2:
            return True
    return False

def is_city_agency(val, col_fname):
    global semantic_set
    if val in semantic_set:
        return True
    return False

def is_subject(val, col_fname):
    global semantic_set
    if val == 'other' and not 'subject' in col_fname:
        return False
    for subject in semantic_set:
        if val == subject or (len(val) > len(subject) and editdistance.eval(val, subject) < 3):
            return True
    return False

def is_areas_study(val, col_fname):
    global semantic_set
    if val in semantic_set:
        return True
    return False

def is_school_level(val, col_fname):
    global semantic_set
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
        semantic_set = [x.strip().lower() for x in semantic_set]

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
        elif semantic == 'PersonName':
            func = is_person_name
        elif semantic == 'BuildingCls':
            func = is_building_cls
        elif semantic == 'Color':
            func = is_color
        elif semantic == 'TypeLocation':
            func = is_type_location
        elif semantic == 'CarMake':
            func = is_car_make
        elif semantic == 'VehicleType':
            func = is_vehicle_type
        elif semantic == 'Borough':
            func = is_borough
        elif semantic == 'Neighborhood':
            func = is_neighborhood
        elif semantic == 'City':
            func = is_city
        elif semantic == 'CityAgency':
            func = is_city_agency
        elif semantic == 'Subject':
            func = is_subject
        elif semantic == 'AreasStudy':
            func = is_areas_study
        elif semantic == 'SchoolLevel':
            func = is_school_level
        elif semantic == 'School':
            func = is_school
        elif semantic == 'Park':
            func = is_park
        elif semantic == 'Address':
            func = is_addr
        elif semantic == 'Street':
            func = is_st
        else:
            func = do_nothing

        for i in range(len(values)):
            val = values[i]
            cell_freq = int(val[1])
            if func(val[0].lower(), column_file_name):
                values[i] = None
                if semantic in pred_labels_temp:
                    pred_labels_temp[semantic] += cell_freq
                else:
                    pred_labels_temp[semantic] = cell_freq
        values = filter_classified(values)
        semantic_set = []

    pred_labels = {}
    for key in pred_labels_temp:
        match_rate = pred_labels_temp[key] / num_items
        if match_rate >= thre or pred_labels_temp[key] > 2000:
            pred_labels[key] = pred_labels_temp[key]
    return pred_labels

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
            col_fname = column_file_name.lower().split('.')[1]
            print(col_fname)
            column_file_name = column_file_name.split('.')

            pred_labels = predict(col_fname, values)
            print(pred_labels)
            col_name = column_file_name[0] + '.' + column_file_name[1]
            output = {"column_name": col_name}
            out_list = []
            for key, val in pred_labels.items():
                out_list.append({"semantic_type": label_dict[key], "count": val})
            output["semantic_types"] = out_list
            print(output)
            with open('../task2out2/{}.json'.format(col_name), 'w+') as outfile:
                json.dump(output, outfile, indent=4)

    # Calculate correct rate
    print(num_columns)