"""
.. module:: MRKmeansDef

MRKmeansDef
*************

:Description: MRKmeansDef



:Authors: bejar


:Version:

:Created on: 17/07/2017 7:42

"""

from __future__ import division,print_function
from mrjob.job import MRJob
from mrjob.step import MRStep
import sys
from collections import defaultdict

import os


__author__ = 'bejar et al.'

def eprint(line):
    with open('/tmp/MRKmeansStep.log', 'a') as f:
        f.write(line + '\n')

class MRKmeansStep(MRJob):
    prototypes = {}

    def jaccard(self, prot, doc):
        """
        Compute here the Jaccard similarity between  a prototype and a document
        prot should be a list of pairs (word, probability)
        doc should be a list of words
        Words must be alphabeticaly ordered

        The result should be always a value in the range [0,1]
        """
        union=0.
        intersection=0.
        i=0
        j=0
        docprot= prot
        while i< len(prot) and j< len(doc):
            if prot[i][0] > doc[j]:
                j+=1
            elif prot[i][0] < doc[j]:
                i+=1
            else:
                intersection+=prot[i][1]
                i+=1
                j+=1
            #this happens regardless
            union+=1

        #the elements that we didn't count must count for the union
        if i< len(docprot) :
            union+= len(docprot) - i
        if j< len(doc):
            union+= len(doc) - i

        #they have the same elements
        if union==intersection:
            return 1
        return intersection/union

    def configure_options(self):
        """
        Additional configuration flag to get the prototypes files

        :return:
        """

        super(MRKmeansStep, self).configure_options()
        self.add_file_option('--prot')

    def load_data(self):
        """
        Loads the current cluster prototypes

        :return:
        """
        f = open(self.options.prot, 'r')
        for line in f:
            cluster, words = line.split(':')
            cp = []
            for word in words.split():
                cp.append((word.split('+')[0], float(word.split('+')[1])))
            self.prototypes[cluster] = cp
        f.close()

    def assign_prototype(self, _, line):
        """
        This is the mapper it should compute the closest prototype to a document
        Words should be sorted alphabetically in the prototypes and the documents
        This function has to return at list of pairs (prototype_id, document words)
        You can add also more elements to the value element, for example the document_id
        """

        # Each line is a string docid:wor1 word2 ... wordn

        doc, words = line.split(':')
        lwords = words.split()

        maxSim = -1.0
        bestCluster = None
        for cluster in self.prototypes:
            sim = self.jaccard(self.prototypes[cluster], lwords)
            if sim > maxSim:
                bestCluster = cluster
                maxSim = sim

        # Return pair key, value
        #eprint('assign out')

        yield bestCluster, [doc, lwords]

    def aggregate_prototype(self, key, values):
        """
        input is cluster and all the documents it has assigned
        Outputs should be at least a pair (cluster, new prototype)

        It should receive a list with all the words of the documents assigned for a cluster

        The value for each word has to be the frequency of the word divided by the number
        of documents assigned to the cluster

        Words are ordered alphabetically but you will have to use an efficient structure to
        compute the frequency of each word

        :param key:
        :param values:
        :return:
        """

        assignments = []
        prototype =  defaultdict(float)
        nDocs = 0

        for [docid, docWords] in values:
            assignments.append(docid)
            for word in docWords:
                prototype[word] += 1
            nDocs += 1

        for word in prototype:
            prototype[word] /= nDocs

        assignments.sort()
        prototype = prototype.items()
        prototype.sort()

        yield key, [assignments, prototype]

    def steps(self):
        return [MRStep(mapper_init=self.load_data, mapper=self.assign_prototype,
                       reducer=self.aggregate_prototype)
            ]


if __name__ == '__main__':
    MRKmeansStep.run()
