# coding:utf-8

from time import time

# number = 1000
def timeit_wrapper(func, *args, **kwargs):
    def wrapped():
        for i in range(1000):
            start = time()
            re = func(*args, **kwargs)
            stop = time()
            print("Stop: " + str(stop))
            print('第{0}次，{1}秒'.format(i, str(stop-start)))
        return re
    return wrapped