from math import inf
import numpy as np
from random import random

"""
  Formalism
  A streaming algorithm consists of 3 parts. 
  * initialization
  * processing step of each element in the stream
  * output step.

"""


class StreamingMinimum:
    def __init__(self):
        self.result = inf

    def update(self, element):
        self.result = min(self.result, element)


class StreamingMean:
    def __init__(self):
        self.result = 0
        self.n = 0

    def update(self, element):
        self.result = ((self.result * self.n) + element) / (self.n + 1)
        self.n += 1


class ReservoirSampler:
    def __init__(self):
        self.result = None
        self.n = 0

    def update(self, element):
        self.n += 1
        if random() < 1 / self.n:
            self.result = element


if __name__ == '__main__':
    stream = iter(np.random.randn(10000))
    s = StreamingMinimum()
    sm = StreamingMean()

    for element in stream:
        s.update(element)
        sm.update(element)
    print(s.result)
    print(sm.result)

    results = []
    for _ in range(1000000):
        r = ReservoirSampler()
        for s in range(20):
            r.update(s)
        results.append(r.result)

"""
References:

https://people.eecs.berkeley.edu/~nirkhe/cs38notes/streaming.pdf

https://towardsdatascience.com/introduction-to-streaming-algorithms-b71808de6d29

Algorithms and Data Structures for Massive Datasets  
Dzejla Medjedovic, Emin Tahirovic, and Ines Dedovic



"""
