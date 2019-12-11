# Combine json files to task2.json

import json
import os

data = {'predicted_types': []}

for file in os.listdir('task2_jsons'):
    if file.endswith('.json'):
        with open('task2_jsons/' + file, 'r') as json_file:
            col_data = json.load(json_file)
            data['predicted_types'].append(col_data)

print (data)
with open('task2.json','w') as json_file:
    json.dump(data, json_file, indent=4)