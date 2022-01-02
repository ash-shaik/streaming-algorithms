from collections import deque


class Memoize:
    def __init__(self, user_function, capacity=2):
        self.fn = user_function
        self.dict = {}
        self.queue = deque([], capacity)

    def __call__(self, n):
        """
        The __call__ method enables Python programmers to write classes where the
        instances behave like functions and can be called like a function.
        :param args:
        :param kwargs:
        :return:
        """
        r = self._get(n)
        if r == -1:
            res_ = self.fn(n)
            self._put(n, res_)
            return res_
        return r

    def _put(self, key, result):
        if key in self.queue:
            self.queue.remove(key)
        self.queue.append(key)
        self.dict[key] = result

    def _get(self, key):
        ## Touch the accessed key to be make it more recent.
        if key in self.queue:
            self.queue.remove(key)
        self.queue.append(key)
        return self.dict.get(key, -1)
