"""
Median - Value that divides such that 50% of data is smaller than the number
         and 50% is greater.
         Requires collection and sorting of all the data.
         When computing this on a stream, we can approximate(estimate) this value
         using stochastic optimization.
"""


class MedianEstimator:

    def __init__(self, rate):
        # rate - learning rate
        self.rate = rate
        self.median = float('inf')

    """
    When the next observed element in the stream is larger than the current estimated 
    median M, we increase M by learning rate.
    When it is smaller, we decrease it by learning rate.
    When M is close to the median it starts stabilizing and 
    - it increases as often as it decreases.  
    """

    def update(self, element):
        if self.median == float('inf'):
            self.median = element
        if self.median == element:
            return self.median
        self.median = self.median + self.rate if self.median < element else -self.rate
        return self.median
