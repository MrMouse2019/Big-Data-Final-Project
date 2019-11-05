#!/bin/bash

# allow match-no-files pattern to expand to a null string
shopt -s nullglob
 
/usr/bin/hadoop fs -find /user/hm74/NYCOpenData  > input
 
while IFS= read -r line; do
	echo "$line"
done < input
 
rm input
