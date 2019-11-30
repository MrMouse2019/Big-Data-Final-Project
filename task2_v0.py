# 1.identify person name
import nltk
import nltk.tag.stanford as st

# tagger = st.StanfordNERTagger('stanford-ner/classifiers/english.all.3class.distsim.crf.ser.gz', 'stanford-ner/stanford-ner.jar')
# text = """Diablo"""
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
# phonenumber = 'input a phone number'
#
# if re.match(phone_pattern, phonenumber):
#     print("{} is a valid phone number".format(phonenumber))
# else:
#     print("{} is not a valid phone number".format(phonenumber))

# 4. address
import os
import pygeocoder
from pygeocoder import Geocoder
from pygeolib import GeocoderError

API_KEY = "geocoder api key"

address0 = "an address"

address1 = Geocoder(API_KEY).geocode(address0)
print(address1.valid_address)




