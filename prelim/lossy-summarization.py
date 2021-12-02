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
Bloom Filter - Data Structure used by a applications to store set membership information.
Trade Off - Possible False Positives
The size of the data strucure allows us to control the false positive rate.
"""
