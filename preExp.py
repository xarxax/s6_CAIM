# this was a failed experiment T_T
# first step creates files documents.txt, vocabulary.txt but won't write to them.

from __future__ import print_function, division
import argparse
import os
import subprocess as sp
import time

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--index', default=None, required=True, help='Index to search')
    parser.add_argument('--minfreq', default=0.0, type=float, required=False, help='Minimum word frequency')
    parser.add_argument('--maxfreq', default=1.0, type=float, required=False, help='Maximum word frequency')
    parser.add_argument('--numwords', default=None, type=int, required=False, help='Number of words')
    parser.add_argument('--nclusts', default="3 5 10", nargs=argparse.REMAINDER, help='List with the numbers of clusters to test')
    args = parser.parse_args()
    
    cwd = os.getcwd()
    dirname = '%s/EXP-minf%.2fmaxf%.2fnumw%d' % (cwd,args.minfreq,args.maxfreq,args.numwords)
    
    try:
        os.rmdir(dirname)
    
    os.mkdir(dirname)
    except: pass

    command = 'python %s/ExtractData.py --index %s --minfreq %d --maxfreq %d --numwords %d' % (cwd,args.index,args.minfreq,args.maxfreq,args.numwords)
    p = sp.Popen(args=command, shell=True, cwd=dirname)
    p_status = p.wait()
    print(p_status)
    
    nclusts = map(int,args.nclusts.split())
    for nclust in nclusts:
        command = 'python %s/GeneratePrototypes.py --nclust %d' % (cwd,nclust)
        p = sp.Popen(sp.Popen(args=command, shell=True, cwd=dirname))
        p_status = p.wait()
        os.rename(dirname+'/prototypes.txt', dirname+'/prototypes%d.txt') % nclust
