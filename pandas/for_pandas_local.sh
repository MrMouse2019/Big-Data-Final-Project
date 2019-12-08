# allow match-no-files pattern to expand to a null string
shopt -s nullglob

while IFS= read -r line; do
        echo "Run dataset: $line ..."
        python task1_pandas_updated.py $line 
done < dataset_names.txt
