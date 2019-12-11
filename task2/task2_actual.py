import json
import csv

filename = 'columns_labeled - Sheet1.csv'

actual = {}
actual['actual_types'] = []
cnt = 0

with open(filename, newline='') as csvfile:
    reader1 = csv.reader(csvfile, delimiter=',')
    for row in reader1:
        cnt += 1
        if cnt == 1:
            continue
        temp = {}
        temp['column_name'] = row[0]
        temp['manual_labels'] = []
        for i in range(1, 6):
            if not row[i]:
                break
            temp1 = {}
            temp1['semantic_type'] = row[i]
            temp['manual_labels'].append(temp1)
        actual['actual_types'].append(temp)

with open('task2-manual-labels.json', 'w') as outfile:
    json.dump(actual, outfile, indent=4)

