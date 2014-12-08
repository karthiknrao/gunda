from gevent import monkey
monkey.patch_all()
from GoogleParser import *
import urllib2
import unirest

import sys

from gevent.pool import Pool

def batch(givenList, batchSize):
    consumedUpto = 0
    while consumedUpto < len(givenList):
        currBatch = []
        while len(currBatch) < batchSize and consumedUpto < len(givenList):
            currBatch.append(givenList[consumedUpto])
            consumedUpto += 1
        yield currBatch

class BatchFetcher:
    def __init__(self, key):
        self.cleanup()
        self.key = key

    def fetch(self, url):
        try:
            crawleraEndpoint = "https://crawlera.p.mashape.com/fetch?url="
            quoteURL = urllib2.quote(url)
            fetchurl = crawleraEndpoint + quoteURL
            response = unirest.get(fetchurl, headers={"X-Mashape-Key": self.key})            
            self.htmlPages.append((url, response))
        except Exception as e:
            sys.stderr.write("ERROR\t" + url + "\t" + str(e) + "\n")
    
    def cleanup(self):
        self.htmlPages = []
        
    def fetchBatch(self, urls, poolSize):
        self.cleanup()
        pool = Pool(min(len(urls), poolSize))
        pool.map(self.fetch, urls)
        return self.htmlPages

def writeResults( fname, urls ):
    with open( outputFileName, 'a' ) as outfile:
        eUrls = [ '\t'.join(x) for x in extractedUrls ]
        outfile.write( '\n'.join(eUrls ) )

inputFilename = sys.argv[1]
outputFileName = sys.argv[2]
key = sys.argv[3]
batchSize = int(sys.argv[4])
poolSize = int(sys.argv[5])

with open(inputFilename) as inputFile:
    lines = inputFile.readlines()
records = mkGoogleSrchUrls([i.strip() for i in lines])
batchFetcher = BatchFetcher(key)
fileCounter = 0

for currBatch in batch(records, batchSize):
    currWebpages = batchFetcher.fetchBatch(currBatch, poolSize)
    extractedUrls = []
    urlsReparse = []
    for record in currWebpages:
        pUrl = record[0]
        htmlPage = record[1].body
        urls = GoogleSS(htmlPage).parse()
        sUrls = splitUrls( urls )
        extractedUrls += zip( [pUrl]*len(sUrls[1]), sUrls[1] )
        urlsReparse += sUrls[0]

    if len(urlsReparse) == 0:
        writeResults( outputFileName, extractedUrls )
        continue
    urlsReFetch = mkGoogleSUUrls( urlsReparse )
    webPagesReparse = batchFetcher.fetchBatch(urlsReFetch, poolSize)
    for record in webPagesReparse:
        pUrl = record[0]
        htmlPage = record[1].body
        urls = GoogleSU(htmlPage).parse()
        extractedUrls += zip( [pUrl]*len(urls), urls )
    writeResults( outputFileName, extractedUrls )

