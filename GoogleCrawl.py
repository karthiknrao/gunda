import urllib
import sys
import random
import time

class GoogleCrawl():
    def __init__( self ):
        self.base_url = "https://www.google.com/search?biw=1440&bih=678&tbs=vw:g,seller:15872&tbm=shop&q=%s"

    def crawl( self, upcs ):
        for upc in upcs:
            url = self.base_url % ( upc )
            with open( upc, 'w' ) as outfile:
                outfile.write( self.fetchUrl( url ) )
            print 'Crawled : ', url
            time.sleep( random.random()*3 )
            
    def fetchUrl( self, url ):
        return urllib.urlopen( url ).read()


if __name__ == '__main__':
    sfile = sys.argv[1]
    def clean(x):
        x = x.strip()
        return '0'*(12-len(x)) + x
    upcs = map( clean, open( sfile ).readlines() )
    #print upcs
    gc = GoogleCrawl()
    gc.crawl( upcs )
