#!/bin/bash

module load python/gnu/3.6.5
module load spark/2.4.0

# allow match-no-files pattern to expand to a null string
shopt -s nullglob

PROGRAM_NAME=task1_test.py
 
#/usr/bin/hadoop fs -find /user/hm74/NYCOpenData  > input

broken_point=${1:-1}
counter=0
while IFS= read -r line; do
	echo "Test dataset: $line ..."
	if (( counter >= broken_point )); then
        /usr/bin/hadoop fs -get "$line"
        name=$(basename "$line")
        gunzip "$name"
        name_unzip=${name%.gz}
        echo "$counter" >> data_first_line.txt
        head -2 "$name_unzip" | tail -1 >> data_first_line.txt
        echo "Removing $name_unzip"
        rm "$name_unzip"
    fi
    ((counter++))
done < input
	 
#rm input
