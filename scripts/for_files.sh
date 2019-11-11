#!/bin/bash

module load python/gnu/3.6.5
module load spark/2.2.0

# allow match-no-files pattern to expand to a null string
shopt -s nullglob

PROGRAM_NAME=for_files_temp.py
 
/usr/bin/hadoop fs -find /user/hm74/NYCOpenData  > input
  
while IFS= read -r line; do
	spark-submit $PROGRAM_NAME $line
	done < input
	 
rm input
