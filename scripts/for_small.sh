#!/bin/bash

module load python/gnu/3.6.5
module load spark/2.4.0

# allow match-no-files pattern to expand to a null string
shopt -s nullglob

/usr/bin/hadoop fs -ls /user/hm74/NYCOpenData | awk '{if ($5 < 50000000) print $8}'  > input

while IFS= read -r line; do
	if [ "$line" != "NYCOpenData" ]; then
		echo "Test dataset: $line ..."
		spark-submit task1_test.py $line
	fi
done < input

rm input
