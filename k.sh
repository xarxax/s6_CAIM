#/opt/elasticsearch-5.4.1/bin/elasticsearch -Epath.conf=../ESconf & 
python IndexFilesPreprocess.py --index news --path ../test --token letter  --filter stop lowercase porter_stem
#python TFIDFViewer.py --index novels --files ../novels/DickensAChristmasCarol.txt ../novels/DickensGreatExpectations.txt --print
#python TFIDFViewer.py --index news --files ../20_newsgroups/alt.atheism/0000000 ../20_newsgroups/alt.atheism/0000000 
