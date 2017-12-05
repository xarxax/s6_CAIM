#python ExtractData.py --index news --minfreq 0.1 --maxfreq 0.3 --numwords 200
#python GeneratePrototypes.py --nclust 3 --data documents.txt
#python MRKmeans.py   --nmaps 1 --nreduces 1 --iter 1
python MRKmeansStep.py document.txt  --prot prototypes.txt < prototypes.txt
