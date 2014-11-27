#! /usr/bin/python
import re

google_base = 'https://www.google.com/search?output=search&tbm=shop&q=%s'
amazon_base = 'http://www.amazon.com/s/ref=nb_sb_noss?url=search-alias%3Daps&field-keywords='

def googleUPC( upc ):
    reg = "[1-9][0-9]+"
    nzupc = re.findall( reg, upc )[0]
    return '0'*(14-len(nzupc)) + nzupc

def amazonUPC( upc ):
    reg = "[1-9][0-9]+"
    nzupc = re.findall( reg, upc )[0]
    if len(nzupc) <= 12:
        return '0'*(12-len(nzupc)) + nzupc
    else:
        return nzupc

lines = open( 'mscurlvalidupcscleaned.tsv' ).readlines()
upcs = []
for line in lines:
    line = line.strip()
    words = line.split( '\t' )
    upcs.append( words[1] )

with open( 'amazonCrawlsMSC.tsv', 'w' ) as outfile:
    for upc in upcs:
        outfile.write( amazon_base + amazonUPC( upc ) + '\n' )

with open( 'googleCrawlMSC.tsv', 'w' ) as outfile:
    for upc in upcs:
        outfile.write( google_base % ( googleUPC( upc ) ) + '\n' )
