#!/bin/bash

module load python/gnu/3.6.5
module load spark/2.4.0

# allow match-no-files pattern to expand to a null string
shopt -s nullglob

/usr/bin/hadoop fs -ls /user/hm74/NYCOpenData | awk '{if ($5 < 500) print $8}'  > input

for i in 1 2 3 4 5; do
	r=$(( $RANDOM % 160 + 2))
	line=$(cat input | head -$r | tail -1) < input

	if [ "$filename" != "NYCOpenData" ]; then
		echo "Test dataset: $filename ..."
		spark-submit task1_test.py $line
	fi
done < input

rm input

