"""
 Memoization is an optimization technique that can be used to speed up runtime
 of programs.
 It works by storing expensive function call results and using its cached results
 when the same call occurs again.

 This is an implementation of a memoizer.


"""


def memoizer(f, k):
    """
    This function
    :param f: function
    :param k: integer representing the number of last distinct results of f to cache.
    :return:
    """
    return f()
