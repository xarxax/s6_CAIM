#python ExtractData.py --index news --minfreq 0.1 --maxfreq 0.3 --numwords 200
#python GeneratePrototypes.py --nclust 20 --data documents.txt
#python MRKmeans.py   --nmaps 1 --nreduces 1 --iter 100
#python MRKmeansStep.py documents.txt  --prot prototypes.txt < prototypes.txt
python MRKmeansStep.py documents.txt  --prot prototypes1.txt < prototypes1.txt
