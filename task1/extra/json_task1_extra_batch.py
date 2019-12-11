import json

def getFilename(path):
    return path.split('/')[-1].split('.')[0]

f = open('task1_datasets_list.txt', 'r')
task1_datasets_list = []
for line in f:
    temp = getFilename(line) + '.json'
    task1_datasets_list.append(temp)
f.close()

for file_name in task1_datasets_list:
    data_dir1 = 'task1_extra_before/'
    path1 = data_dir1 + file_name
    with open(path1) as json_file:
        data = json.load(json_file)

    data_dir2 = 'task1_ordered/'
    path2 = data_dir2 + file_name
    with open(path2) as json_file:
        data1 = json.load(json_file)

    data1['key_column_candidates'] = data['key_column_candidates']

    extra_dir = 'task1_extra_after/'
    path3 = extra_dir + file_name
    with open(path3, 'w') as outfile:
        json.dump(data1, outfile, indent=4)

