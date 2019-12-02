import sys

def process(ds_name):
	input = 'list_all_datasets.txt'
	d_name = ds_name[1:10]
	col = ds_name[11: -9]
	d_path = '/user/hm74/NYCOpenData/' + d_name + '.tsv.gz'
	print (d_name, ' ', col, ' ', d_path)
	'''
	with open(input, 'r') as file:
		while True:
			line = file.readline()
			if not line:
				break
			if len(line) != 94:
				continue
			ds = line[-17:-8]
			if ds == d_name:
				#print(ds)
				#print(ds_name)
				#print (line)
				pass
	'''
	return


filename = 'list_task2_datasets.txt'
with open(filename, 'r') as f:
	while True:
		dataset_name = f.readline()
		if not dataset_name:
			break
		process(dataset_name)
