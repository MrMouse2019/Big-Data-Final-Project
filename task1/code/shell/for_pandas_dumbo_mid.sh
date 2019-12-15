module load python/gnu/3.6.5

# allow match-no-files pattern to expand to a null string
shopt -s nullglob

PROGRAM_NAME=task1_pandas_updated_mid.py

while IFS= read -r line; do
        echo "Test dataset: $line ..."
        /usr/bin/hadoop fs -get $line
        name=$(basename "$line")
        spark-submit $PROGRAM_NAME $name 
        rm $name
done < mid_size_datasets.txt
