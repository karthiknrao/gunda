from bs4 import BeautifulSoup
import re, sys, urllib
import glob

class GoogleSU():
    """/shopping/product/ parser"""
    def __init__( self, data ):
        self.soup = BeautifulSoup( data, 'lxml' )
        tg = self.soup.findAll( "span", "pag-n-to-n-txt" )
        self.products = int( re.findall( 'of ([0-9]+)', tg[0].text )[0] )
        if self.products > 10:
            self.allprods = self.soup.findAll( "a", "pag-detail-link" )[0]['href']
            ndata = self.fetchUrl( 'http://www.google.com' + self.allprods )
            self.soup = BeautifulSoup( ndata, 'lxml' )
            
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

if __name__ == '__main__':
    files = glob.glob( 'GoogleCrawls21/*.html' )
    for fil in files:
        data = open( fil ).read()
        gp = GoogleSS(data)
        links = gp.parse()
        if len( links ) > 0:
            def prourl( x ):
                return fil + '\t' + x
            plinks = map( prourl, links )
            sys.stdout.write( '\n'.join(plinks) + '\n' )
