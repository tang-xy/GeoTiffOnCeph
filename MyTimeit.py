# coding:utf-8

from time import time

# number = 100
def timeit_wrapper(func, *args, **kwargs):
    def wrapped():
        for i in range(100):
            start = time()
            re = func(*args, **kwargs)
            stop = time()
            print('第{0}次，{1}秒'.format(i, str(stop-start)))
        return re
    return wrapped