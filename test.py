import pandas as pd
import re
fw = open('/home/kps/Desktop/outputtabs_unique.txt','w')
i = 0
words_set = dict()
with open('/home/kps/Desktop/first10.tsv','r') as f:
  for line in f:
    r = line.split("\t")
    words_set[r[1]] = 1
with open('/home/kps/Desktop/first10.tsv','r') as f:
  for line in f:	
    r = line.split("\t")
    matches = re.findall('<target>[A-Za-z ]*</target>', r[3], re.DOTALL)
    matches = [list[8:-9] for list in matches]
    for list in matches:
      if list in words_set and list != r[1]:
        list = r[1]+"\t"+ list
        fw.write("%s\n"%list)
    i = i+1
    print(i)
