# Convert old label names to snake_case labels
import json

def convert_col_name(val):
	val = val.lower()
	if val == 'LAT/LON coordinates'.lower():
		val = 'lat_lon_cord'
	if val == 'School Levels'.lower():
		val = 'school_level'
	if val == 'Websites'.lower():
		val = 'website'
	if val == 'Type of Location'.lower():
		val = 'location_type'
	if val == 'Parks/Playgrounds'.lower():
		val = 'park_playground'
	if val == 'College/University names'.lower():
		val = 'college_name'
	if val == 'Subjects in School'.lower():
		val = 'subject_in_school'
	if val == 'Areas of study'.lower():
		val = 'area_of_study'
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

