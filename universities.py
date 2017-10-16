import re
fw = open('/home/kps/workspace/spark/src/universities.txt','w')
i = 0
words_set = dict()
with open('/home/kps/workspace/spark/src/2_3gb.txt','r') as f:
  for line in f:
    matches = re.findall(".*(niversity|nstitute|nstitution).*",line,re.DOTALL)
    if len(matches) != 0:
      fw.write(line)
    i = i+1
    print(i)

