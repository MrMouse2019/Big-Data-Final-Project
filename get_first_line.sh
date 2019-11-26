#!/bin/bash

module load python/gnu/3.6.5
module load spark/2.4.0

# allow match-no-files pattern to expand to a null string
shopt -s nullglob

PROGRAM_NAME=get_first_line.py
 
/usr/bin/hadoop fs -find /user/hm74/NYCOpenData  > input

while IFS= read -r line; do
	echo "Test dataset: $line ..."
	if [ "$line" != "/user/hm74/NYCOpenData" ]; then
		spark-submit $PROGRAM_NAME $line
	fi
done < input
	 
rm input
