import re
import csv

source = '../task2data/columns_labeled.csv'
if __name__ == "__main__":
    with open(source, newline='') as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            print("%d\t%s\t%s"%(len(row), row[0], row[1]))
