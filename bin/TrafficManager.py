import numpy as np
from math import ceil, floor

class TrafficManager(object):
    _instance      = None
    _init          = False

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super(TrafficManager,cls).__new__(cls, *args, **kwargs)
        return cls._instance

    def  __init__(self, mu = None, sigma = None, randomSeed = None):
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
        self.trafficStd = sigma # standard deviation

    def getThroughput(self):
        pick = -1.0
        while pick <= 0.0:
            pick = np.random.normal(self.trafficAverage, self.trafficStd, None)
            if pick < self.trafficAverage:
                pick = ceil(pick)
            else:
                pick = floor(pick)
        return pick

def main():
    TM = TrafficManager(30, 10, 11)
    picks = []
    for x in range(25):
        picks.append(TM.getThroughput())
    print picks
    print np.mean(picks)

if __name__=="__main__":
    main()
