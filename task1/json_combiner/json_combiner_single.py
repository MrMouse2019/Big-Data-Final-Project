import json

json1 = '2k3g-r445.json'
json2 = '4pt5-3vv4.json'
res = []
with open(json1) as outfile:
    res.append(json.load(outfile))
with open(json2) as outfile:
    res.append(json.load(outfile))
with open('task1.json', 'w') as outfile:
    json.dump(res, outfile, indent=4)