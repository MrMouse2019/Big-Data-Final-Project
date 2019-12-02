module load python/gnu/3.6.5

# allow match-no-files pattern to expand to a null string
shopt -s nullglob

PROGRAM_NAME=task1_pandas.py

/usr/bin/hadoop fs -ls /user/hm74/NYCOpenData | awk '{if ($5 >= 50000000 && $5 <= 100000000) print $8}'  > input

while IFS= read -r line; do
        echo "Test dataset: $line ..."
        python task1_pandas.py $line 
done < input

rm input
