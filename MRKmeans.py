"""
.. module:: MRKmeans

MRKmeans
*************

:Description: MRKmeans

    Iterates the MRKmeansStep script

:Authors: bejar
    

:Version: 

:Created on: 17/07/2017 10:16 

"""
from __future__ import print_function, division

from MRKmeansStep import MRKmeansStep
import shutil
import argparse
import os
import time


__author__ = 'bejar et al.'

def saveAssignments(i,assign):
    f = open('assignments%d.txt' % i, 'w')
    
    clusters = sorted(assign)
    for cluster in clusters:
        docvec = ''
        for docid in assign[cluster]:
            docvec += (docid + ' ')  
        f.write(cluster + ':' + docvec.encode('ascii','replace') + '\n')
        
    f.flush()
    f.close()

def savePrototypes(i,proto):
    f = open('prototypes%d.txt'% i, 'w')
    
    clusters = sorted(proto)
    for cluster in clusters:
        wordvec = ''
        print()
        for (word,freq) in proto[cluster]:
            wordvec += (word + '+%d ' % freq)
        f.write(cluster + ':' + wordvec.encode('ascii','replace') + '\n')
        
    f.flush()
    f.close()


def savePrototypes2(i,proto):
    f = open('prototypes%d.txt'% i, 'w')
    
    for cluster in proto:
        wordvec = ''
        for [word,freq] in proto[cluster]:
            wordvec += (word + '+%d ' % freq)
        f.write(cluster+ ':' + wordvec.encode('ascii','replace') + '\n')
        
    f.flush()
    f.close()


def loadAssignments(i):
    f = open('assignments%d.txt' % i, 'r')
    
    assign = dict()
    for line in f:
        cluster, docvec = line.split(':')
        cp = []
        for docid in docvec.split():
            cp.append(docid)
        assign[cluster] = cp
    
    return assign

def equal(assign1, assign2):
    for cluster in assign1:
        if assign1[cluster] != assign2[cluster]: return False
    return True

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--prot', default='prototypes.txt', help='Initial prototpes file')
    parser.add_argument('--docs', default='documents.txt', help='Documents data')
    parser.add_argument('--iter', default=5, type=int, help='Number of iterations')
    parser.add_argument('--nmaps', default=2, type=int, help='Number of parallel map processes to use')
    parser.add_argument('--nreduces', default=2, type=int, help='Number of parallel reduce processes to use')

    args = parser.parse_args()
    assign = {}

    # Copies the initial prototypes
    cwd = os.getcwd()
    shutil.copy(cwd + '/' + args.prot, cwd + '/prototypes0.txt')

    nomove = False  # Stores if there has been changes in the current iteration
    for i in range(args.iter):
        tinit = time.time()  # For timing the iterations

        # Configures the script
        print('Iteration %d ...' % (i + 1))
        # The --file flag tells to MRjob to copy the file to HADOOP
        # The --prot flag tells to MRKmeansStep where to load the prototypes from
        mr_job1 = MRKmeansStep(args=['-r', 'local', args.docs,
                                     '--file', cwd + '/prototypes%d.txt' % i,
                                     '--prot', cwd + '/prototypes%d.txt' % i,
                                     '--jobconf', 'mapreduce.job.maps=%d' % args.nmaps,
                                     '--jobconf', 'mapreduce.job.reduces=%d' % args.nreduces])

        # Runs the script
        print('hello')
        with mr_job1.make_runner() as runner1:
            print('running')
            runner1.run()
            print('ran')
            new_assign = {}
            new_proto = {}
            old_proto ={}
            # Process the results of the script, each line one results
            for line in runner1.stream_output():
                #cluster, [assignments, prototype] = mr_job1.parse_output_line(line)
                cluster,prototype = mr_job1.parse_output_line(line)
                #print(type(yiel))
                #print('iter',i,'cluster',cluster)
                # You should store things here probably in a datastructure
                #new_assign[cluster] = assignments
                new_proto[cluster] = prototype
            # If your scripts returns the new assignments you could write them in a file here
            # You should store the new prototypes here for the next iteration
            #saveAssignments(i+1,new_assign)
            print(new_proto)
            savePrototypes2(i+1,new_proto)

            # If you have saved the assignments, you can check if they have changed from the previous iteration
            #old_assign = loadAssignments(i)
            nomove = new_proto == old_proto
            old_proto=new_proto
            

        print("Time= %f seconds" % (time.time() - tinit))

        if nomove:  # If there is no changes in two consecutive iterations we can stop
            print("Algorithm converged")
            break

    # Now the last prototype file should have the results
