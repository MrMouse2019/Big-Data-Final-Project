import json

def getFilename(path):
    return path.split('/')[-1].split('.')[0]

f = open('task1_datasets_list.txt', 'r')
task1_datasets_list = []
for line in f:
    temp = getFilename(line) + '.json'
    task1_datasets_list.append(temp)
f.close()

json_dir = 'task1_json_12091441/'
res = []
for filename in task1_datasets_list:
    path = json_dir + filename
    with open(path) as outfile:
        res.append(json.load(outfile))

with open('task1.json', 'w') as outfile:
    json.dump(res, outfile, indent=4)
