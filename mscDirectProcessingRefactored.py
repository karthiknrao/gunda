import hadoopy
import sys
import os
import re
from collections import defaultdict

class Record:
    def __init__(self, line):
        self.url, self.title, self.image_url, self.asin, self.upc,\
        self.standardized_upc, self.brand,self.brandid,self.mpn,self.sku,\
        self.standardized_attributes = line.split('\t')
    def tsv(self):
        return "\t".join([self.url, self.title, self.image_url, self.asin, self.upc,\
                         self.standardized_upc, self.brand, self.mpn,self.sku,\
                         self.standardized_attributes])
class AlternateMPN:
    def __init__(self, line):
        self.MSCItem, self.PrimarySupplierID, self.PrimarySupplierName,\
        self.VendorPartNumber, self.AdditionalPartNumber = line.split('\t')
class AlternateBrand:
    def __init__(self, line):
        self.supplier, self.brand, self.altBrand = line.split('\t')

def stdUPC( upc ):
    reg = '[1-9][0-9]+'
    nzupc = re.findall( reg, upc )[0]
    if len(nzupc) <= 14:
        return '0'*(14-len(nzupc)) + nzupc
    else:
        return nzupc

def extractBrandIDMap(key, value):
    value = value.strip()
    if len(value.split('\t')) == 2:
           brand,brandid = value.split('\t')
           if len(brand) > 0 and len(brandid) > 0:
               yield brand, brandid

def extractBrandIDReduce(key, values):
    valuesList = []
    for value in values:
        valuesList.append(value)
    brandid = valuesList[0]
    yield key, brandid

def graingerBVMap(key, value ):
    words = value.split('\t')
    upc = words[11]
    url = words[9]
    if "www.grainger.com" in url:
        #words[11] = stdUPC( upc )
        yield key, '\t'.join(words)

def graingerBVReduce(key, values ):
    listValues = []
    for value in values:
        listValues.append( value )
        
    yield key, listValues[0]

def graingerBVToMSCMap(key, value):
    words = value.split('\t')
    rec = [words[9],words[4],words[10],'N/A','','INVALID_UPC',words[5],\
           words[15],words[3],'N/A']
    yield key, '\t'.join(rec)

def graingerBVToMSCReduce(key, values):
    valuesList = []
    for value in values:
        valuesList.append(value)
    if len(valuesList) > 0:
        yield key, valuesList[0]

def mpnJoinMapper(key, value):
    if "data/MSCDirect" in os.environ['map_input_file']:
        record = Record(value)
        if "www.mscdirect.com" in record.url:
            yield record.sku, ("DATA", value)
    elif "mscMeta/mscDirectMultipleMPN" in os.environ['map_input_file']:
        alternateMPN = AlternateMPN(value)
        yield alternateMPN.MSCItem, ("META", value)

def mpnJoinReducer(key, values):
    recordMap = defaultdict(list)
    for value in values:
        recordType, recordValue = value
        recordMap[recordType].append(recordValue)
    if "META" in recordMap:
        alternateMPN = AlternateMPN(recordMap["META"][0])
        for line in recordMap["DATA"]:
            record = Record(line)
            record.mpn = alternateMPN.AdditionalPartNumber
            yield None, record.tsv()

def alternateBrandMap(key, value):
    if "data/MSCDirect" in os.environ['map_input_file']:
        record = Record(value)
        if "www.mscdirect.com" in record.url:
            if len(record.brand) > 0:
                yield record.brand.lower(), ("DATA", value)
    elif "mscMeta/AlternateBrand" in os.environ['map_input_file']:
        alternateBrand = AlternateBrand(value)
        if len(alternateBrand.brand.strip()) > 0 and len(alternateBrand.altBrand.strip()) > 0:
            yield alternateBrand.brand.strip().lower(), ("META", value)

def alternateBrandReduce(key, values):
    recordMap = defaultdict(list)
    for value in values:
        recordType, recordValue = value
        recordMap[recordType].append(recordValue)
    if "META" in recordMap:
        for rec in recordMap["META"]:
            alternateBrand = AlternateBrand(rec)
            for line in recordMap["DATA"]:
                record = Record(line)
                record.brand = alternateBrand.altBrand
                yield None, record.tsv()

def fixMPNMap(key, value):
    record = Record(value)
    if "www.mscdirect.com" not in record.url:
        if len(record.mpn) > 0 and record.mpn.lower() not in [ 'na', 'n/a', 'null' ]:
            if record.standardized_upc.strip() != 'INVALID_UPC':
                yield record.standardized_upc, ( 'MPN', value )
        else:
            if record.standardized_upc.strip() != 'INVALID_UPC':
                yield record.standardized_upc, ( 'NOMPN', value )

def fixMPNReduce(key, values):
    recordMap = defaultdict(list)
    for value in values:
        rType, rec = value
        recordMap[rType].append(rec)
    if len(recordMap['MPN']) > 0:
        for rec in recordMap['MPN']:
            recd = Record(rec)
            mpn = recd.mpn
            for line in recordMap['NOMPN']:
                norec = Record(line)
                norec.mpn = mpn
                yield None, norec.tsv()

def mscFilterMap(key, value):
    record = Record(value)
    if "www.mscdirect.com" in record.url:
        yield None, value

def extractStoreSKUPairs(lines):
    store2SKU = {}
    for line in lines:
        record = Record(line)
        store = record.url.split('/')[2]
        store2SKU[store] = record.sku
    mscdirectSKU = store2SKU["www.mscdirect.com"]
    result = []
    for store, sku in store2SKU.items():
        if store != "www.mscdirect.com":
            result.append(("www.mscdirect.com", mscdirectSKU, store, sku))
    return result

def makeUrlPairs(lines):
    storeRec = {}
    for line in lines:
        record = Record(line)
        store = record.url.split('/')[2]
        storeRec[store] = line
    mscDirectRec = storeRec["www.mscdirect.com"]
    result = []
    for store, rec in storeRec.items():
        if store != "www.mscdirect.com":
            result.append((mscDirectRec, rec) )
    return result
        

def brandMPNMap(key, value):
    record = Record(value)
    if len(record.brand) > 0:
        yield (record.brand.lower(), record.mpn.lower()), value

def productAtStoreReduce(key, values):
    valuesList = []
    storeSet = set([])
    for value in values:
        valuesList.append(value)
        record = Record(value)
        store= record.url.split('/')[2]
        storeSet.add(store)
    if len(storeSet) > 1 and "www.mscdirect.com" in storeSet:
        pairs = extractStoreSKUPairs(valuesList)
        for pair in pairs:
            yield pair, None

def productAtStoreWithRecReduce(key, values):
    valuesList = []
    storeSet = set([])
    for value in values:
        valuesList.append(value)
        record = Record(value)
        store= record.url.split('/')[2]
        storeSet.add(store)
    if len(storeSet) > 1 and "www.mscdirect.com" in storeSet:
        recPairs = makeUrlPairs(valuesList)
        for pair in recPairs:
            yield pair, None

def upcMap(key, value):
    record = Record(value)
    if record.standardized_upc.strip() != "INVALID_UPC":
        yield record.standardized_upc, value

def dumbMap(key, value):
    yield key, None

def dumbReduce(key, values):
    yield key, None

def oneForwardMap(key, value):
    store1, sku1, store2, sku2 = key
    yield (store1, sku1, store2), sku2
      
def oneForwardReduce(key, values):
    store1, sku1, store2 = key
    valueList = []
    for value in values:
        valueList.append(value)
    sku2 = valueList[0]
    yield (store1, sku1, store2, sku2), None

def oneForwardRecMap(key, value):
    ( rec1, rec2 ) = key
    record1 = Record(rec1)
    record2 = Record(rec2)
    store1 = record1.url.split('/')[2]
    store2 = record2.url.split('/')[2]
    sku1 = record1.sku
    sku2 = record2.sku
    yield ( store1, sku1, store2 ), (sku2, rec1, rec2 )

def oneForwardRecReduce(key, values):
    store1, sku1, store2 = key
    valueList = []
    for value in values:
        valueList.append(value)
    valid = valueList[0]
    (sku2, rec1, rec2 ) = valid
    yield ( rec1, rec2 ), None

def oneBackwardRecMap(key, value):
    ( rec1, rec2 ) = key
    record1 = Record(rec1)
    record2 = Record(rec2)
    store1 = record1.url.split('/')[2]
    store2 = record2.url.split('/')[2]
    sku1 = record1.sku
    sku2 = record2.sku
    if store2 == 'www.toolup.com':
        yield( store2, record2.url, store1 ), ( sku1, rec1, rec2 )
    else:
        yield ( store2, sku2, store1 ), (sku1, rec1, rec2 )

def oneBackwardRecReduce(key, values):
    store2, sku2, store1 = key
    valueList = []
    for value in values:
        valueList.append(value)
    valid = valueList[0]
    (sku1, rec1, rec2 ) = valid
    yield ( rec1, rec2 ), None

def oneBackwardMap(key, value):
    store1, sku1, store2, sku2 = key
    yield (store2, sku2, store1), sku1

def oneBackwardReduce(key, values):
    store2, sku2, store1 = key
    valueList = []
    for value in values:
        valueList.append(value)
    sku1 = valueList[0]
    yield (store1, sku1, store2, sku2), None

def summaryMap(key, value):
    yield (key[0], key[2]), 1

def summaryReduce(key, values):
    total = 0
    for value in values:
        total += value
    yield key, total

def filterMSCMatchesMap(key, value):
    words = value.split('\t')
    if "www.mscdirect.com" in words[0] or "www.mscdirect.com" in words[1]:
        yield key, value

def filterMSCMatchesReduce(key, values):
    valuesList =  []
    for value in values:
        valuesList.append(value)
    for value in valuesList:
        yield None, value

def mpidPairsMap(key, value):
    words = value.strip().split('\t')
    if len(words) == 3:
        mpid = words[2]
        yield mpid, words[0]
        yield mpid, words[1]

def mpidPairsReduce(key, values):
    stores = [ "www.toolup.com",
            "www.grainger.com",
            "www.fastenal.com",
            "www.zoro.com",
            "www.homedepot.com",
            "www.lawsonproducts.com",
            "www.amazonsupply.com",
            "www.mscdirect.com"
           ]
    storesUrls = defaultdict(list)
    for value in values:
        store = value.split('/')[2]
        if store in stores:
            storesUrls[store].append(value)
    if len(storesUrls["www.mscdirect.com"]) > 0:
        mscUrl = storesUrls["www.mscdirect.com"][0]
        for store in stores:
            if store != "www.mscdirect.com":
                for url in storesUrls[store]:
                    yield (mscUrl,url), None
    

executionPlan = []
#executionPlan.append(('/data/bazaarvoiceFinal/destination.tsv', graingerBVMap, graingerBVReduce,'/data/graingerFromBV'))
#executionPlan.append(('/data/graingerFromBV', graingerBVToMSCMap, graingerBVToMSCReduce,'/data/graingerBVMSCFormat'))
#executionPlan.append((['/data/MSCDirect','/data/mscMeta/AlternateBrand'], alternateBrandMap, alternateBrandReduce, '/data/MSCDirectAlternateBrand'))
#executionPlan.append(('/data/MSCDirect', fixMPNMap, fixMPNReduce, '/data/MSCDirectMPNFromUPC'))
"""
executionPlan.append((['/data/MSCDirect','/data/mscMeta/mscDirectMultipleMPN.tsv'], mpnJoinMapper, mpnJoinReducer, '/data/MSCDirectAlternateMPN'))
executionPlan.append((['/data/MSCDirectAlternateMPN','/data/graingerBVMSCFormat', '/data/MSCDirect'], brandMPNMap, productAtStoreReduce, '/data/BrandMPNMAtchesAlternate'))
executionPlan.append((['/data/MSCDirect','/data/graingerBVMSCFormat', '/data/MSCDirectAlternateBrand', '/data/MSCDirectMPNFromUPC'], brandMPNMap, productAtStoreReduce, '/data/BrandMPNMAtches'))
executionPlan.append(('/data/MSCDirect', upcMap, productAtStoreReduce, '/data/UPCMAtches'))
executionPlan.append((['/data/UPCMAtches', '/data/BrandMPNMAtches', '/data/BrandMPNMAtchesAlternate'], dumbMap, dumbReduce, '/data/UniqueMatches'))
"""
"""
executionPlan.append((['/data/MSCDirectAlternateMPN','/data/graingerBVMSCFormat', '/data/MSCDirect'], brandMPNMap, productAtStoreWithRecReduce, '/data/BrandMPNMAtchesAlternateWithRec'))
executionPlan.append((['/data/MSCDirect','/data/graingerBVMSCFormat', '/data/MSCDirectAlternateBrand', '/data/MSCDirectMPNFromUPC'], brandMPNMap, productAtStoreWithRecReduce, '/data/BrandMPNMAtchesWithRec'))
executionPlan.append(('/data/MSCDirect', upcMap, productAtStoreWithRecReduce, '/data/UPCMAtchesWithRec'))
executionPlan.append((['/data/UPCMAtchesWithRec', '/data/BrandMPNMAtchesWithRec', '/data/BrandMPNMAtchesAlternateWithRec'], dumbMap, dumbReduce, '/data/UniqueMatchesWithRec'))
"""
"""
executionPlan.append(('/data/UniqueMatches', oneForwardMap, oneForwardReduce, '/data/OneForwardMatches'))
executionPlan.append(('/data/OneForwardMatches', oneBackwardMap, oneBackwardReduce, '/data/MatchesAtStore'))
executionPlan.append(('/data/MatchesAtStore', summaryMap, summaryReduce, '/data/MatchesAtStoreSummary'))
"""

#executionPlan.append(('/data/UniqueMatchesWithRec', oneForwardRecMap, oneForwardRecReduce, '/data/OneForwardMatchesWithRec'))
#executionPlan.append(('/data/OneForwardMatchesWithRec', oneBackwardRecMap, oneBackwardRecReduce, '/data/MatchesAtStoreWithRec'))
#executionPlan.append(('/data/MatchesAtStoreWithRec', summaryMap, summaryReduce, '/data/MatchesAtStoreSummaryWithRec'))

#executionPlan.append(('/data/MSCDirectBrands', extractBrandIDMap, extractBrandIDReduce, '/data/MSCDirectExtractedBrands'))
executionPlan.append(('/data/finalmatches/finalmatches', mpidPairsMap, mpidPairsReduce, '/data/finalmatchesMSCfiltered'))

inputs,mapperStages,reducerStages,outputs = zip(*executionPlan)
thisFilename = "/home/hadoop/MSCDirect/brandMPN/mscDirectProcessingRefactored.py"

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
