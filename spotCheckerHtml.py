#! /usr/bin/python

import random
import sys
import time

class CreateHtmlPage():
    def __init__( self, headers, fname ):
        self.page = ''
        self.header = '<!DOCTYPE html>\n<html>\n<body>\n<table style="width:100%" border="1">\n'
        self.footer = '\n</table>\n</body>\n</html>'

        self.rowtemplate = '\n<tr>%s\n</tr>\n'
        self.colimg = '\n<td><img src=%s style="width:128px;height:128px"</td>'
        self.colname = '\n<td>%s</td>'

        self.coln = len( headers ) + 1
        self.tableHeaders = ''.join( [ self.colname % ( 'SNo' ) ] +\
                            [ self.colname % ( x ) for x in headers ] +\
                            [ self.colname % ( 'Click' )] )
        self.recn = 0
        self.js = "ele = getElementById('demo%s');if(ele.innerHTML == 'Right') { ele.innerHTML = 'Wrong'; } else { ele.innerHTML = 'Right'; }"
        self.button = "<button  onclick=\"%s\">Eval</button><p id=\"demo%s\">Right</p>"
        self.outname = fname + '_SpotChecker%d.html'
        
    def createRow( self, rowele ):
        def createCol(x):
            if x[:4] == 'http':
                return self.colname % ('<a href="%s">Url</a>' % (x))
            else:
                return self.colname % x
        self.jc = self.js % ( str(self.recn) )
        self.button1 = self.button % ( self.jc, str(self.recn) )
        row = ''.join( [ self.colname % ( str(self.recn) ) ] +\
                [ createCol(x) for x in rowele ] +\
                [ self.colname % (self.button1 )])
        self.recn += 1
        return self.rowtemplate % ( row )

    def addRecord( self, records ):      
        for rec in records:
            self.page = self.page + self.createRow( rec )

    def writePage( self ):
        self.page = self.header + self.tableHeaders +\
            self.page + self.footer
        outFile = self.outname % ( int(time.time()) )
        with open( outFile, 'w' ) as outfile:
            outfile.write( self.page )

    def refreshPage( self ):
        self.page = ''


class RandomSampler():
    def __init__( self, fname, seed, sampleSize ):
        self.input = fname
        random.seed( seed )
        self.sample = sampleSize

    def fetchRandomSample( self ):
        records = []
        self.lines = open( self.input ).readlines()
        for i in range( self.sample ):
            randomLine = random.randint(0, len( self.lines ) )
            records.append( self.lines[randomLine] )
        return records

class Columnizer():
    def __init__( self, cols ):
        self.cols = cols

    def splitRec( self, rec ):
        rec = rec.strip()
        words = rec.split( '\t' )
        return [ words[i] for i in self.cols ]

if __name__ == '__main__':

    seed = sys.argv[1]
    samples = int(sys.argv[2])
    inpname = sys.argv[3]
    #sample = RandomSampler( inpname, seed, samples )
    records = open( inpname ).readlines()
    #records = sample.fetchRandomSample()
    columns = [ Columnizer([0,1,3,-1]).splitRec(x) for x in records ]
    htmlwrite = CreateHtmlPage(['Sku','Upc','Title', 'Url'], inpname)
    htmlwrite.refreshPage()
    htmlwrite.addRecord( columns )
    htmlwrite.writePage()
