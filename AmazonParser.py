from bs4 import BeautifulSoup
import re, sys, urllib
import glob

class Amazon():
    """Amazon Search Parser"""
    def __init__( self, data ):
        self.soup = BeautifulSoup( data, 'lxml' )

    def parse( self ):
        prods = self.soup.findAll( 'a', 'a-link-normal s-access-detail-page a-text-normal' )
        urls = [ prod['href'] for prod in prods ]
        return urls

if __name__ == '__main__':
    data = open( sys.argv[1] ).read()
    amz = Amazon( data )
    print amz.parse()
