module load python/gnu/3.6.5
moduel load pandas

# allow match-no-files pattern to expand to a null string
shopt -s nullglob

PROGRAM_NAME=task1_pandas_old_date_parser.py

while IFS= read -r line; do
        echo "Test dataset: $line ..."
        /usr/bin/hadoop fs -get /$line
        name=$(basename "$line")
        spark-submit $PROGRAM_NAME $name 
        rm $name
done < large_datasets.txt
