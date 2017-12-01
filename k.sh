python ExtractData.py --index news --minfreq 0.1 --maxfreq 0.3 --numwords 200
pyhton GeneratePrototypes.py --nclust 3 --data documents.txt
pyhton MRKmeans.py   --nmaps 1 --nreduces 1 
