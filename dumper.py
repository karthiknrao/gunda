import hadoopy
import unicodedata

# data = hadoopy.readtb('/data/StoreCounts')
data = hadoopy.readtb('/data/MSCUrlUpc')
for i, j in data:
    print i, j
# unicodedata.normalize('NFKD', j).encode('ascii','ignore')
