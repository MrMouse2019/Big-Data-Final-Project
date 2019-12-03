# allow match-no-files pattern to expand to a null string
shopt -s nullglob

mkdir task2_datasets_unzip
while IFS= read -r line; do
	echo Getting column: /user/hm74/NYCColumns/$line
	/usr/bin/hadoop fs -get /user/hm74/NYCColumns/$line
	mv $line task2_datasets
	gunzip task2_datasets_unzip/$line
done < list_task2_datasets.txt

