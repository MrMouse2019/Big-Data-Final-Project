#!/bin/bash
input="./list_task2_datasets.txt"
while IFS= read -r line
do
  temp="${line%\'}"
  temp="${temp#\'}"
  echo "$temp" >> list_task2_datasets_.txt
done < "$input"
