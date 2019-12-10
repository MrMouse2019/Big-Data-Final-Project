import re
import sys

str = sys.argv[1].lower()
print(re.search('(\. +)|( +\.)', str))
