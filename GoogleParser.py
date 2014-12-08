from bs4 import BeautifulSoup
import re, sys, urllib
import glob

def googleUPC( upc ):
    reg = "[1-9][0-9]+"
    nzupc = re.findall( reg, upc )[0]
    return '0'*(14-len(nzupc)) + nzupc

def mkGoogleSUUrls( urls ):
    base_url = 'http://www.google.com/%s/online?q=%s'
    reg_url = '/(shopping/product/[0-9]+)?'
    reg_upc = 'q=([0-9]+)'
    def fixUrl( x ):
        shp = re.findall( reg_url, x )
        upc = re.findall( reg_upc, x )
        furl = base_url % ( shp[0], upc[0] )
        return furl
    return map( fixUrl, urls )

def splitUrls( urls ):
    sp = [ x for x in urls if x.find('/shopping/product/') != -1 ]
    ss = [ x for x in urls if x.find('/shopping/product/') == -1 ]
    return ( sp , ss )

def mkGoogleSrchUrls( upcs ):
    base_url = 'https://www.google.com/search?output=search&tbm=shop&q=%s'
    return [ base_url % ( googleUPC(x) ) for x in upcs ]

class GoogleSU():
    """/shopping/product/ parser"""
    def __init__( self, data ):
        self.soup = BeautifulSoup( data, 'lxml' )
            
    def parse( self ):
        self.prods = self.soup.findAll( "span", "os-seller-name-primary" )
        urls = [ prod.findAll('a')[0]['href'] for prod in self.prods ]
        return map( lambda x : 'http://www.google.com' + x, urls )
            
    def fetchUrl( self, url ):
        return urllib.urlopen( url ).read()

class GoogleSS():
    """Shopping Search Results Parser"""
    def __init__( self, data ):
        self.soup = BeautifulSoup( data, 'lxml' )

    def parse( self ):
        shpu = self.soup.findAll( "a", "_po" )
        durl = self.soup.findAll( "h3", "r" )
        links = [ x['href'] for x in shpu ] +\
          [ x.findAll('a')[0]['href'] for x in durl ]
        return links
