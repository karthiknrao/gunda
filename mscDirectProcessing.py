import hadoopy
import sys
import os
from collections import defaultdict

fields = {"url":0,"title":1,"image_url":2,"asin":3,"upc":4,"standardized_upc":5,"brand":6,"mpn":7,"sku":8,"standardized_attributes":9}

def mpnJoinMapper(key, value):
    if "data/MSCDirect" in os.environ['map_input_file']:
        words = value.split('\t')
        if "www.mscdirect.com" in words[fields["url"]]:
            yield words[fields["sku"]], ("DATA", value)
    elif "mscMeta/mscDirectMultipleMPN" in os.environ['map_input_file']:
        words = value.split('\t')
        yield words[0], ("META", value)

def mpnJoinReducer(key, values):
    recordMap = defaultdict(list)
    for value in values:
        recordType, recordValue = value
        recordMap[recordType].append(recordValue)
    if "META" in recordMap:
        MSCItem, PrimarySupplierID, PrimarySupplierName,VendorPartNumber, AdditionalPartNumber = recordMap["META"][0].split('\t') 
        for record in recordMap["DATA"]:
            words = record.split('\t')
            words[fields["mpn"]] = AdditionalPartNumber
            yield None, "\t".join(words)

def mscFilterMap(key, value):
    words = value.split('\t')
    if "www.mscdirect.com" in words[fields["url"]]:
        yield None, value

def extractStoreSKUPairs(lines):
    store2SKU = {}
    for line in lines:
        words = line.split('\t')
        store = words[fields["url"]].split('/')[2]
        store2SKU[store] = words[fields["sku"]]
    mscdirectSKU = store2SKU["www.mscdirect.com"]
    result = []
    for store, sku in store2SKU.items():
        if store != "www.mscdirect.com":
            result.append(("www.mscdirect.com", mscdirectSKU, store, sku))
    return result

def brandMPNMap(key, value):
    words = value.split('\t')
    if len(words[fields["brand"]]) > 0:
        yield (words[fields["brand"]], words[fields["mpn"]]), value

def productAtStoreReduce(key, values):
    valuesList = []
    storeSet = set([])
    for value in values:
        valuesList.append(value)
        store = value.split('\t')[fields["url"]].split('/')[2]
        storeSet.add(store)
    if len(storeSet) > 1 and "www.mscdirect.com" in storeSet:
        pairs = extractStoreSKUPairs(valuesList)
        for pair in pairs:
            yield pair, None

def upcMap(key, value):
    words = value.split('\t')
    if words[fields["standardized_upc"]].strip() != "INVALID_UPC":
        yield words[fields["standardized_upc"]], value

def dumbMap(key, value):
    yield key, None

def dumbReduce(key, values):
    yield key, None

def summaryMap(key, value):
    yield (key[0], key[2]), 1

def summaryReduce(key, values):
    total = 0
    for value in values:
        total += value
    yield key, total

executionPlan = []
executionPlan.append((['/data/MSCDirect', '/data/mscMeta/mscDirectMultipleMPN.tsv'], mpnJoinMapper, mpnJoinReducer, '/data/MSCDirectAlternateMPN'))
executionPlan.append((['/data/MSCDirectAlternateMPN', '/data/MSCDirect'], brandMPNMap, productAtStoreReduce, '/data/BrandMPNMAtchesAlternate'))
executionPlan.append(('/data/MSCDirect', brandMPNMap, productAtStoreReduce, '/data/BrandMPNMAtches'))
executionPlan.append(('/data/MSCDirect', upcMap, productAtStoreReduce, '/data/UPCMAtches'))
executionPlan.append((['/data/UPCMAtches', '/data/BrandMPNMAtches', '/data/BrandMPNMAtchesAlternate'], dumbMap, dumbReduce, '/data/MatchesAtStore'))
executionPlan.append(('/data/MatchesAtStore', summaryMap, summaryReduce, '/data/MatchesAtStoreSummary'))
inputs,mapperStages,reducerStages,outputs = zip(*executionPlan)
thisFilename = "/home/hadoop/MSCDirect/brandMPN/mscDirectProcessing.py"

def mapper(key, value):
    stageCounter = 0
    for mapperStage in mapperStages:
        stageCounter += 1
        if os.environ["stage"] == "stage" + str(stageCounter):
            for key, value in mapperStage(key, value):
                yield key, value

def reducer(key, value):
    stageCounter = 0
    for reducerStage in reducerStages:
        stageCounter += 1
        if os.environ["stage"] == "stage" + str(stageCounter):
            for key, value in reducerStage(key, value):
                yield key, value

if __name__ == '__main__':
    if sys.argv[1] == "launch":
        for i in xrange(0, len(mapperStages)):
            jobconfs = {}
            jobconfs["stage"] = "stage" + str(i + 1)
            hadoopy.launch(inputs[i], outputs[i], thisFilename, remove_output = True, jobconfs=jobconfs)
    else:
        hadoopy.run(mapper, reducer)
