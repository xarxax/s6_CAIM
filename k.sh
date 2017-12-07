#python ExtractData.py --index news --minfreq 0.1 --maxfreq 0.3 --numwords 200
#python GeneratePrototypes.py --nclust 20 --data documents.txt
rm /tmp/MRKmeansStep.log
python MRKmeans.py --nmaps 4 --nreduces 4 --iter 5
#python MRKmeansStep.py documents.txt  --prot prototypes.txt < prototypes.txt
