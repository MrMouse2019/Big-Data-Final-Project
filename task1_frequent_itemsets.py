import pandas as pd
import json
from mlxtend.preprocessing import TransactionEncoder
from mlxtend.frequent_patterns import apriori

# dataset = [['Milk', 'Onion', 'Nutmeg', 'Kidney Beans', 'Eggs', 'Yogurt'],
#            ['Dill', 'Onion', 'Nutmeg', 'Kidney Beans', 'Eggs', 'Yogurt'],
#            ['Milk', 'Apple', 'Kidney Beans', 'Eggs'],
#            ['Milk', 'Unicorn', 'Corn', 'Kidney Beans', 'Yogurt'],
#            ['Corn', 'Onion', 'Onion', 'Kidney Beans', 'Ice cream', 'Eggs']]
#
# te = TransactionEncoder()
#
# te_ary = te.fit(dataset).transform(dataset)
#
# df = pd.DataFrame(te_ary, columns=te.columns_)
#
# frequent_itemsets = apriori(df, min_support=0.5, use_colnames=True)
#
# print(frequent_itemsets)

with open('task1.json') as json_file:
    data = json.load(json_file)
# print(len(data))

A = []
for dataset in data:
    for column in dataset['columns']:
        temp = []
        for data_type in column['data_types']:
            temp.append(data_type['type'])
        A.append(temp)

# print(len(A))

te = TransactionEncoder()
te_ary = te.fit(A).transform(A)
df = pd.DataFrame(te_ary, columns=te.columns_)
frequent_itemsets = apriori(df, min_support=0.01, use_colnames=True).sort_values(by=['support'], ascending=False)
frequent_itemsets = frequent_itemsets.reset_index(drop=True)
print(frequent_itemsets)
# id = 0
# for idx in frequent_itemsets.index:
#     id += 1
#     print(id, frequent_itemsets['support'].iloc[idx], frequent_itemsets['itemsets'].iloc[idx])