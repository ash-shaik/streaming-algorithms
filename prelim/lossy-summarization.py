"""
 Sketch algorithms were developed to approximate answers to questions such as:
 1. Set membership
 2. Cardinality estimation
 3. Frequency.

 1. Update in the order of O(1)
 2. Storage space is independent of the size of the stream
 3. Read in linear time.

 Downside:
 1. Tolerable Errors in results.

 Algorithms:
 1. Bloom Filters.
 2. Min Count
 3. HyperLogLog
 4. Count-Min Sketch

Building Blocks:
1. Registers - counters that store the data for the sketch.
   Does not depend on the number of input elements.
2. Hash Functions - Deterministic functions that take an input and return a value in some finite
   space of m output values. Speed is the primary requirement of these functions in application
   of sketches.
   Murmur hash is a popular variant used.
"""

"""
Bloom Filter - Data Structure used by a applications to store set membership information
 (Test if an element is part of a large set, without needing to store all elements of the set).
Trade Off - Accuracy, Possible False Positives
The size of the data strucure allows us to control the false positive rate.
"""

import mmh3
from bitarray import bitarray


class BloomSet(object):

    def __init__(self, m, seeds):
        self.m = m
        self.size_used = 0
        self.num_hashes = len(seeds)
        # is an array of bits, set to zero initially.
        self.__bits = bitarray(self.m)
        self.__bits.setall(0)

    def add(self, item):
        if item not in self and self.size_used < self.m:
            for i in range(self.num_hashes):
                index = mmh3.hash(item, i) % self.m
                self.__bits[index] = 1
            self.size_used += 1

    def contains(self, item):
        for i in range(self.num_hashes):
            index = mmh3.hash(item, i) % self.m
            if not self.__bits[index]:
                return False
            return True
