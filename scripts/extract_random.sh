#!/bin/bash

module load python/gnu/3.6.5
module load spark/2.2.0

# allow match-no-files pattern to expand to a null string
shopt -s nullglob

/usr/bin/hadoop fs -find /user/hm74/NYCOpenData  > input

for i in 1 2 3 4 5; do
	r=$(( $RANDOM % 1902 + 1))
	line=$(cat input | head -$r | tail -1) < input
	filename=$(basename $line | sed 's/\.[^.]+$//')

	if [ "$filename" != "NYCOpenData" ]; then
		echo "copy $filename ..."
		/usr/bin/hadoop fs -getmerge $line $filename
	fi
done < input

rm input

