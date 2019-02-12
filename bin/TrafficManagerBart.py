import numpy as np
from math import ceil, floor
import math
import random

class TrafficManagerBart(object):
    _instance      = None
    _init          = False

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super(TrafficManagerBart,cls).__new__(cls, *args, **kwargs)
        return cls._instance

    def  __init__(self, numMotes = None, mu = None, randomSeed = None):
        '''
            mu is the throughputs
            sigma is the std deviation
            random seed is an optional random seed
        '''

        if self._init:
            return
        self._init = True

        if randomSeed != None:
            np.random.seed(int(randomSeed*1000))
        self.trafficAverage = mu # mean

        self.listTX = []

        self.dictHalf = {}
        self.dictHalf[3] = 1.5
        self.dictHalf[5] = 2.5
        self.dictHalf[10] = 5.0
        self.dictHalf[30] = 15.0
        self.dictHalf[60] = 30.0

        self.dictDouble = {}
        self.dictDouble[3] = 6.0
        self.dictDouble[5] = 10.0
        self.dictDouble[10] = 20.0
        self.dictDouble[30] = 60.0
        self.dictDouble[60] = 120.0

        indexMote = 0
        while indexMote < numMotes:
            if indexMote < 5:
                self.listTX.append(self.dictHalf[self.trafficAverage])
            elif 5 <= indexMote < 15:
                self.listTX.append(self.trafficAverage)
            elif 15 <= indexMote < 25:
                self.listTX.append(self.dictDouble[self.trafficAverage])
            indexMote += 1

        random.shuffle(self.listTX)
        print self.listTX

    def getThroughput(self):
        if len(self.listTX) > 0:
            return self.listTX.pop()
        else:
            raise BaseException('Not enough TXs in the list.')

def main():
    TM = TrafficManager(25, 10, 11)
    print TM.getThroughput()
    print TM.getThroughput()

if __name__=="__main__":
    main()
