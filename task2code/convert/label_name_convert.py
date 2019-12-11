# Convert old label names to snake_case labels
import json

def convert_col_name(val):
	if val == 'LAT/LON coordinates':
		val = 'lat_lon_cord'
	if val == 'School Levels':
		val = 'school_level'
	if val == 'Websites':
		val = 'website'
	if val == 'Type of Location':
		val = 'location_type'
	if val == 'Parks/Playgrounds':
		val = 'park_playground'
	if val == 'College/University names':
		val = 'college_name'
	if val == 'Subjects in School':
		val = 'subject_in_school'
	val = val.lower()
	val = val.replace(' ', '_')
	return val


with open('task2-manual-labels.json','r')as json_file:
    data = json.load(json_file)

columns = data['actual_types']
for column in columns:
	column['column_name'] = column['column_name'][:-4]
	for label in column['manual_labels']:
		label['semantic_type'] = convert_col_name(label['semantic_type'])

with open('task2-manual-labels-converted.json','w')as json_file:
    json.dump(data, json_file, indent=4)
