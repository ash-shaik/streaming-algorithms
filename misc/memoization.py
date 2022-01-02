"""
 Memoization is an optimization technique that can be used to speed up runtime
 of programs.
 It works by storing expensive function call results and using its cached results
 when the same call occurs again.

 This is an implementation of a memoizer.


"""
import time
from misc.Memoize import Memoize


def fibonacci(n):
    if n == 0:
        return 0
    if n == 1:
        return 1
    else:
        return fibonacci(n - 1) + fibonacci(n - 2)


def test_without_memo():
    start = time.time()
    result = fibonacci(35)
    time_taken = time.time() - start
    print(result)
    print('Time taken ', time_taken, ' seconds')


def test_with_memo(n):
    start = time.time()
    result = fibonacci_m(n)
    time_taken = time.time() - start
    print(result)
    print('Time taken ', time_taken, ' seconds')


def memoize(capacity=2):
    """
    A wrapper method to memoize the decorated function.
    :param capacity:
    :return:
    """

    def _memoizer(function):
        return Memoize(function, capacity)

    return _memoizer


@memoize(10)
def fibonacci_m(n):
    if n == 0:
        return 0
    if n == 1:
        return 1
    else:
        return fibonacci(n - 1) + fibonacci(n - 2)


if __name__ == '__main__':
    print("Without Memo")
    test_without_memo()

    print("With Memo")
    test_with_memo(35)

    print("Without Memo")
    test_without_memo()

    print("With Memo")
    test_with_memo(35)
    test_with_memo(36)
