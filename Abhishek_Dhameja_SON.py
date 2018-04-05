import sys
import os
import itertools
from pyspark import SparkContext


def getSingleFrequents(baskets,s):
    support=len(baskets)*s
    result=[]
    countTable={}
    for basket in baskets:
        for item in basket:
            countTable.setdefault(item,0)
            countTable[item]+=1
    # print support
    # print countTable
    for item,count in countTable.iteritems():
        if count>=support:
            result.append(item)
    return sorted(result)

def getFrequents(baskets,s,previousfrequents,size):
    support=len(baskets)*s
    counttable={}
    result=[]
    candidates=[]
    if size==2:
        for candidate in itertools.combinations(previousfrequents,size):
            candidates.append(candidate)
    else:
        #print 'previous frequents=',previousfrequents
        for item in itertools.combinations(previousfrequents, 2):
            if len(set(item[0]).intersection(set(item[1]))) == size-2:
                candidate=tuple(sorted(set(item[0]).union(set(item[1]))))
                #print 'candidate=',candidate
                if candidate in candidates:
                    continue
                else:
                    temp = itertools.combinations(candidate, size-1)
                    if set(temp).issubset(previousfrequents):
                        #print 'appending',candidate
                        candidates.append(candidate)

    #print candidates
    for candidate in candidates:
        for basket in baskets:
            if set(candidate).issubset(basket):
                counttable.setdefault(candidate,0)
                counttable[candidate]+=1
    #print counttable
    for candidate,count in counttable.iteritems():
        if count>=support:
            result.append(candidate)
    #print sorted(result)
    return sorted(result)


def apriori(iterator):
    baskets = []
    for v in iterator:
        baskets.append(v)
    freqItems=[]
    size=1
    singlefrequents=getSingleFrequents(baskets,s)
    for item in singlefrequents:
        freqItems.append(item)
    size+=1
    currentfrequents=singlefrequents;
    while True:
        previousfrequents=currentfrequents
        currentfrequents=getFrequents(baskets,s,previousfrequents,size)
        for item in currentfrequents:
            freqItems.append(item)
        if len(currentfrequents)==0:
            break
        size += 1
    return freqItems


if __name__ == "__main__":
    if len(sys.argv) != 4:
        print 'Usage: bin/spark-submit Abhishek_Dhameja_SON.py baskets.txt <support> <output.txt>'
        exit(-1)

    inputfile = sys.argv[1]
    s = float(sys.argv[2])
    outputfile = sys.argv[3]

    sc = SparkContext(appName='SON_APRIORI')
    
    lines = sc.textFile(inputfile).map(lambda line: line.strip().split(',')).map(lambda line:map(int,line))
    
    result = lines.mapPartitions(apriori)


    support=lines.count()*s
    frequents=[]
    if lines.getNumPartitions()==1:
        for item in result.collect():
            frequents.append(item)
    else:
        for item in result.collect():
            if item in frequents:
                continue
            count=0
            for line in lines.collect():
                if type(item) is tuple:
                    if set(item).issubset(line):
                        count+=1
                else:
                    if item in line:
                        count+=1

            if count>=support:
                frequents.append(item)

    f = open(os.path.join(outputfile), 'w')
    for item in frequents:
        if type(item) is tuple:
            length=len(item)
            f.write("(")
            for i in range(0,length-1):
                f.write(str(item[i])+",")
            f.write(str(item[length-1])+")\n")
        else:
            f.write(str(item) + "\n")
    f.close()
