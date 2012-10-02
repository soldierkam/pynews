# -*- coding: utf-8 *-*
__author__ = 'soldier'



class IncDict(object):

    def __init__(self):
        self.__d = {}
        self.__keys = {}

    def inc(self, elem, step = 1):
        if elem in self.__d:
            self.__d[elem] += step
            return (self.__keys[elem], True)
        else:
            self.__d[elem] = 1
            self.__keys[elem]=elem
            return (elem, False)

    def remove(self, elem):
        del self.__d[elem]

    def items(self):
        return self.__d.items()

    def __iter__(self):
        return self.__d.__iter__();

    def __str__(self):
        return "".join(map(str, self.__d))


class LimitedCollection(dict):

    def __init__(self, max_size, min_size):
        self.__logic_time = 0
        self.__max_size = max_size
        self.__min_size = min_size

    def __setitem__(self, key, value):

        super(LimitedCollection, self).__setitem__(key, value)