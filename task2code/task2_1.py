# 1.identify person name
# import nltk
# import nltk.tag.stanford as st

# tagger = st.StanfordNERTagger('stanford-ner/classifiers/english.all.3class.distsim.crf.ser.gz', 'stanford-ner/stanford-ner.jar')
#
# text = """ZHANG"""
# flag = True
#
# for sent in nltk.sent_tokenize(text):
#     tokens = nltk.tokenize.word_tokenize(sent)
#     tags = tagger.tag(tokens)
#     # print(tags)
#     for tag in tags:
#         if tag[1]!='PERSON':
#             flag = False
#             break
#
# if flag:
#     print("{} is a person name".format(text))
# else:
#     print("{} is not a person name".format(text))



# 2. business name
# cleanco



# 3. phone number
import re

# phone_pattern = re.compile(r'^\(?([0-9]{3})\)?[-.●]?([0-9]{3})[-.●]?([0-9]{4})$')
# phonenumber = '(347)249-6790'
#
# if re.match(phone_pattern, phonenumber):
#     print("{} is a valid phone number".format(phonenumber))
# else:
#     print("{} is not a valid phone number".format(phonenumber))

# 4. address
import os
# import pygeocoder
# from pygeocoder import Geocoder
# from pygeolib import GeocoderError

# API_KEY = "AIzaSyBtu01qoEwyrqe4WI14eJKWhPxMivdIalo"
#
# address0 = "86 Fleet Pl, Brooklyn, NY 11201"
#
# address1 = Geocoder(API_KEY).geocode(address0)
# print(address1.valid_address)

# cluster2.txt to array
# task2_file_list = []
# with open('instructions/cluster2.txt', 'r') as fp:
#     task2_file_list = fp.readlines()
#
# str0 = task2_file_list[0][1:-1]
# # print(str0)
# task2_file_list = str0.split(', ')
# print(len(task2_file_list))
# print(task2_file_list)
#
# f = open('task2_dataset_list.txt', 'w')
# for dataset in task2_file_list:
#     f.write(dataset)
#     f.write('\n')
# f.close()

# 5. school name
line1 = 'The George Washington University, Washington, DC, USA.'
line2 = 'Department of Pathology, University of Oklahoma Health Sciences Center, Oklahoma City, USA. adekunle-adesina@ouhsc.edu'

matchlist = ['Hospital','University','Institute','School','Academy', 'College'] # define all keywords that you need look up
p = re.compile('^(.*?),\s+(.*?),(.*?)\.')   # regex pattern to match

# We use a list comprehension using 'any' function to check if any of the item in the matchlist can be found in either group1 or group2 of the pattern match results
def is_school(pattern, line):
    res = [m.group(1) if any(x in m.group(1) for x in matchlist) else m.group(2) for m in re.finditer(pattern,line)]
    if res:
        return True
    return False
# line1match = [m.group(1) if any(x in m.group(1) for x in matchlist) else m.group(2) for m in re.finditer(p,line1)]
# line2match = [m.group(1) if any(x in m.group(1) for x in matchlist) else m.group(2) for m in re.finditer(p,line2)]
# print (line1match)
# print (line2match)

if is_school(p, line2):
    print("School!")
else:
    print("Not a school!")





