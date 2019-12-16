module load python/gnu/3.6.5

# allow match-no-files pattern to expand to a null string
shopt -s nullglob

PROGRAM_NAME=get_key_candidates.py

while IFS= read -r line; do
        echo "Run dataset: $line ..."
        /usr/bin/hadoop fs -get $line
        name=$(basename "$line")
        spark-submit $PROGRAM_NAME $name 
        rm $name
done < datasets.txt
