# -*- coding: utf-8 *-*
__author__ = 'soldier'



class IncDict(object):

    def __init__(self):
        self.__d = {}

    def inc(self, elem, step = 1):
        if elem in self.__d:
            self.__d[elem] += step
        else:
            self.__d[elem] = 1

    def remove(self, elem):
        del self.__d[elem]

    def __str__(self):
        return str(self.__d)


class LimitedCollection(dict):

    def __init__(self, max_size, min_size):
        self.__logic_time = 0
        self.__max_size = max_size
        self.__min_size = min_size

    def __setitem__(self, key, value):

        super(LimitedCollection, self).__setitem__(key, value)