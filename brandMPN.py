import hadoopy
import sys
import os
from collections import defaultdict

def stage1Map(key, value):
    words = value.split('\t')
    if len(words[6]) > 0:
        yield (words[6], words[7]), value

def stage1Reduce(key, values):
    valuesList = []
    for value in values:
        valuesList.append(value)
    uniqueAtStore = []
    storeDict = defaultdict(list)
    for record in valuesList:
        store = record.split('\t')[0].split('/')[2]
        storeDict[store].append(record)
    storeSet = set(storeDict.keys())
    if len(storeSet) > 1 and "www.mscdirect.com" in storeSet:
        storeSet.remove("www.mscdirect.com")
        yield key, storeSet

def stage2Map(key, value):
    for store in value:
        yield store, key

def stage2Reduce(key, values):
    valuesList = []
    for value in values:
        valuesList.append(value)
    yield key, len(valuesList)

#########
def stage3Map( key, value ):
    words = value.split( '\t' )
    if len(words) > 6:
        store = words[0].split('/')[2]
        if store == 'www.mscdirect.com':
            yield key,  ( words[0], words[5] )

def stage3Reduce(key, values):
    valuesList = []
    for value in values:
        valuesList.append(value)
    for value in valuesList:
        yield key, value

########
def stage4Map(key, value):
    words = value.split( '\t' )
    if len(words) > 6:
        if words[5].find( 'INVALID_UPC' ) == -1:
            yield words[5], value

def stage4Reduce(key, values):
    otherstores = []
    mscdirect = []
    for value in values:
        if value.find('www.mscdirect.com' ) != -1:
            mscdirect.append( value )
        else:
            otherstores.append( value )
    if len( mscdirect ) > 0:
        mscprod = mscdirect[0]
        for store in otherstores:
            yield key, (mscprod, store )

######
def mapper(key, value):
    if os.environ['stage'] == 'stage1':
        for key, value in stage1Map(key, value):
            yield key, value
    elif os.environ['stage'] == 'stage2':
        for key, value in stage2Map(key, value):
            yield key, value
    elif os.environ['stage'] == 'stage3':
        for key, value in stage3Map(key, value):
            yield key, value
    elif os.environ['stage'] == 'stage4':
        for key, value in stage4Map(key, value):
            yield key, value

def reducer(key, values):
    if os.environ['stage'] == 'stage1':
        for key, value in stage1Reduce(key, values):
            yield key, value
    elif os.environ['stage'] == 'stage2':
        for key, value in stage2Reduce(key, values):
            yield key, value
    elif os.environ['stage'] == 'stage3':
        for key, value in stage3Reduce(key, values):
            yield key, value
    elif os.environ['stage'] == 'stage4':
        for key, value in stage4Reduce(key, values):
            yield key, value

if __name__ == '__main__':
    if sys.argv[1] == "launch":
#        hadoopy.launch('/data/MSCDirect', '/data/MSCDirectBrandMPN', '/home/hadoop/MSCDirect/brandMPN/brandMPN.py', remove_output = True, jobconfs={'stage':'stage1'})
#        hadoopy.launch('/data/MSCDirectBrandMPN', '/data/StoreCounts', '/home/hadoop/MSCDirect/brandMPN/brandMPN.py', remove_output = True, jobconfs={'stage':'stage2'})
        #hadoopy.launch('/data/MSCDirect', '/data/MSCUrlUpc', '/home/hadoop/MSCDirect/brandMPN/brandMPN.py', remove_output = True, jobconfs={'stage':'stage3'})
        hadoopy.launch('/data/MSCDirect', '/data/MSCUpcMatches', '/home/hadoop/MSCDirect/brandMPN/brandMPN.py', remove_output = True, jobconfs={'stage':'stage4'})
        data = hadoopy.readtb('/data/MSCUpcMatches')
        for i, j in data:
            print i, j
    else:
        hadoopy.run(mapper, reducer)

