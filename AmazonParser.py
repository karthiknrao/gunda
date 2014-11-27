from bs4 import BeautifulSoup
import re, sys, urllib
import glob

class Amazon():
    """Amazon Search Parser"""
    def __init__( self, data ):
        self.soup = BeautifulSoup( data, 'lxml' )

    def parse( self ):
        prods = self.soup.findAll( 'div', 'a-fixed-left-grid-col a-col-right' )
        data = []
        for prod in prods:
            url = prod.findAll( 'a', 'a-link-normal s-access-detail-page a-text-normal' )[0]['href']
            price = prod.findAll( 'span', 'a-size-base a-color-price s-price a-text-bold' )[0].text
            data.append( ( url, price ) )
        return data

if __name__ == '__main__':
    data = open( sys.argv[1] ).read()
    amz = Amazon( data )
    for url, price in amz.parse():
        print url, price
    
