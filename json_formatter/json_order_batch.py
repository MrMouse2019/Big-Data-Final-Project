import json
import os

def getFilename(path):
    return path.split('/')[-1].split('.')[0]

# f = open('task1_datasets_list.txt', 'r')
# f = open('list1.txt', 'r')
f = open('small_size_datasets.txt', 'r')
# f = open('mid_size_datasets.txt', 'r')
# f = open('large_size_datasets.txt', 'r')
# f = open('xlarge_size_datasets.txt', 'r')
task1_datasets_list = []
for line in f:
    temp = getFilename(line) + '.json'
    task1_datasets_list.append(temp)
f.close()

unordered_dir = 'task1_unordered/small/'
ordered_dir = 'task1_ordered/small/'
# unordered_dir = 'task1_unordered/mid/'
# ordered_dir = 'task1_ordered/mid/'
# unordered_dir = 'task1_unordered/large/'
# ordered_dir = 'task1_ordered/large/'
# unordered_dir = 'task1_unordered/xlarge/'
# ordered_dir = 'task1_ordered/xlarge/'


for file_name in task1_datasets_list:

    path1 = unordered_dir + file_name
    path2 = ordered_dir + file_name

    # if os.path.exists(path2):
    #     print("{} has been ordered!".format(file_name))
    #     continue

    with open(path1) as json_file:
        data = json.load(json_file)

    data1 = {}

    data1['dataset_name'] = data['dataset_name']

    data1['columns'] = []

    for column in data['columns']:
        column1 = {}
        column1['column_name'] = column['column_name']
        column1['number_non_empty_cells'] = column['number_non_empty_cells']
        column1['number_empty_cells'] = column['number_empty_cells']
        column1['number_distinct_values'] = column['number_distinct_values']
        column1['frequent_values'] = column['frequent_values']
        column1['data_types'] = []
        for type in column['data_type']:
            type1 = {}
            if type['type'] == 'INTEGER':
                type1['type'] = type['type']
                type1['count'] = type['count']
                type1['max_value'] = type['max_value']
                type1['min_value'] = type['min_value']
                type1['mean'] = type['mean']
                type1['stddev'] = type['stddev']
            elif type['type'] == 'REAL':
                type1['type'] = type['type']
                type1['count'] = type['count']
                type1['max_value'] = type['max_value']
                type1['min_value'] = type['min_value']
                type1['mean'] = type['mean']
                type1['stddev'] = type['stddev']
            elif type['type'] == 'DATE/TIME':
                type1['type'] = type['type']
                type1['count'] = type['count']
                type1['max_value'] = type['max_value']
                type1['min_value'] = type['min_value']
            elif type['type'] == 'TEXT':
                type1['type'] = type['type']
                type1['count'] = type['count']
                type1['shortest_values'] = type['shortest_values']
                type1['longest_values'] = type['longest_values']
                type1['average_length'] = type['average_length']
            if type1:
                column1['data_types'].append(type1)
        data1['columns'].append(column1)

    data1['key_column_candidates'] = data['key_column_candidates']

    # for item in data1:
        # print(item)

    # for column in data1['columns']:
    #     for item in column:
    #         print(item)

    with open(path2, 'w') as outfile:
        json.dump(data1, outfile, indent=4)

