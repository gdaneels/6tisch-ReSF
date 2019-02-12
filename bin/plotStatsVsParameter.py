#!/usr/bin/python
'''
\brief Plots statistics with a selected parameter. 

\author Thomas Watteyne <watteyne@eecs.berkeley.edu>
\author Kazushi Muraoka <k-muraoka@eecs.berkeley.edu>
\author Nicola Accettura <nicola.accettura@eecs.berkeley.edu>
\author Xavier Vilajosana <xvilajosana@eecs.berkeley.edu>
'''

#============================ adjust path =====================================

#============================ logging =========================================

import logging
class NullHandler(logging.Handler):
    def emit(self, record):
        pass
log = logging.getLogger('plotStatsVsParameter')
log.setLevel(logging.ERROR)
log.addHandler(NullHandler())

#============================ imports =========================================

import os
import re
import glob
import sys
import math

import numpy
import scipy
import scipy.stats

import logging.config
import matplotlib.pyplot
import argparse

#============================ defines =========================================

CONFINT         = 0.95
START_CYCLE     = 100 # statistics (reliability, packet loss, latency) are collected after this cycle

COLORS = [
   '#0000ff', #'b'
   '#ff0000', #'r'
   '#008000', #'g'
   '#bf00bf', #'m'
   '#000000', #'k'
   '#ff0000', #'r'

]

LINESTYLE = [
    '--',
    '-.',
    ':',
    '-',
    '-',
    '-',
]

ECOLORS= [
   '#0000ff', #'b'
   '#ff0000', #'r'
   '#008000', #'g'
   '#bf00bf', #'m'
   '#000000', #'k'
   '#ff0000', #'r'
]

#============================ body ============================================

def parseCliOptions():
    
    parser = argparse.ArgumentParser()
    
    parser.add_argument( '--elemName',
        dest       = 'elemName',
        type       = str,
        default    = 'numMotes',
        help       = 'Name of the element to be used as x axis.',
    )

    parser.add_argument( '--statsName',
        dest       = 'statsName',
        type       = str,
        default    = 'numTxCells',
        help       = 'Name of the statistics to be used as y axis.',
    )
    
    options        = parser.parse_args()
    
    return options.__dict__

#===== end-to-end reliabilities

def plot_reliability(elemName):
    
    dataSetDirs = []
    for dataSetDir in os.listdir(os.path.curdir):
        if os.path.isdir(dataSetDir):
            dataSetDirs += [dataSetDir]
    
    reliabilities      = {}
    
    # One curve is generated by each dataSetDir
    for dataSetDir in dataSetDirs: 

        reliabilitiesEachSet = {}
        # assume that each data file in dataSetDir uses same parameters other than a value of elemName
        for dir in os.listdir(dataSetDir):
            
            for infilename in glob.glob(os.path.join(dataSetDir, dir,'*.dat')):
                
                elem, reliabilitiesEachElem = calcReliability(
                    dir                = os.path.join(dataSetDir, dir),
                    infilename         = os.path.basename(infilename),
                    elemName           = elemName
                )
                
                reliabilitiesEachSet[elem]    = reliabilitiesEachElem
                                
        reliabilities[dataSetDir] = reliabilitiesEachSet
            

    if elemName == 'numMotes':
        xlabel = 'number of motes'
    elif elemName == 'pkPeriod':
        xlabel = 'packet period (s)'
    elif elemName == 'otfThreshold':
        xlabel = 'OTF threshold'
    elif elemName == 'sixtopPdrThreshold':
        xlabel = 'threshold for cell relocation'
    elif elemName == 'numChans':
        xlabel = 'number of channels'
    else:
        xlabel = elemName
    
    outfilename    = 'output_reliability_{}'.format(elemName)
    
    # plot figure for e2e reliability   
    genStatsVsParameterPlots(
        reliabilities,
        dirs           = dataSetDirs,
        outfilename    = outfilename,
        xlabel         = xlabel,
        ylabel         = 'end-to-end reliability',
    )

    outfilename    = 'output_loss_{}'.format(elemName)

    bar_reliabilities = {}

    # One curve is generated by each dataSetDir
    for dataSetDir in dataSetDirs:
        bar_reliabilitiesEachSet = {}
        for elem, reliabilitiesEachElem in reliabilities[dataSetDir].items():
            bar_reliabilitiesEachSet[elem] = [1.0-v for v in reliabilitiesEachElem]
        bar_reliabilities[dataSetDir] = bar_reliabilitiesEachSet
        
    # plot figure for packet drop rate   
    genStatsVsParameterPlots(
        bar_reliabilities,
        dirs           = dataSetDirs,
        outfilename    = outfilename,
        #xmin           = 20,#1.0
        #xmax           = 150,#3.0,
        #ymin           = 0.0005,
        ymax           = 0.1,
        xlabel         = xlabel,
        ylabel         = 'end-to-end packet loss ratio',
        log            = True,
    )

def calcReliability(dir,infilename,elemName):
    
    infilepath     = os.path.join(dir,infilename)
        
    # find xAxis, numCyclesPerRun    
    with open(infilepath,'r') as f:
        for line in f:
            
            if line.startswith('##'):
                
                # elem
                m = re.search(elemName+'\s+=\s+([\.0-9]+)',line)
                if m:
                    elem               = float(m.group(1))
                
                # numCyclesPerRun
                m = re.search('numCyclesPerRun\s+=\s+([\.0-9]+)',line)
                if m:
                    numCyclesPerRun    = int(m.group(1))
    
    # find appGenerated, appReachesDagroot, txQueueFill, colnumcycle
    with open(infilepath,'r') as f:
        for line in f:
            if line.startswith('# '):
                elems                   = re.sub(' +',' ',line[2:]).split()
                numcols                 = len(elems)
                colnumappGenerated      = elems.index('appGenerated')
                colnumappReachesDagroot = elems.index('appReachesDagroot')
                colnumtxQueueFill       = elems.index('txQueueFill')
                colnumcycle             = elems.index('cycle')
                colnumrunNum            = elems.index('runNum')                
                break
        
    assert numCyclesPerRun > START_CYCLE
    
    # parse data
    reliabilities  = []
    totalGenerated = 0
    totalReaches   = 0
    previousCycle  = None
    with open(infilepath,'r') as f:
        for line in f:
            if line.startswith('#') or not line.strip():
                continue
            m = re.search('\s+'.join(['([\.0-9]+)']*numcols),line.strip())
            appGenerated        = int(m.group(colnumappGenerated+1))
            appReachesDagroot   = int(m.group(colnumappReachesDagroot+1))
            txQueueFill         = int(m.group(colnumtxQueueFill+1))      
            cycle               = int(m.group(colnumcycle+1))
            runNum              = int(m.group(colnumrunNum+1))
            
            if cycle==START_CYCLE-1:
                initTxQueueFill     = txQueueFill
                
            if cycle>=START_CYCLE:
                totalGenerated     += appGenerated
                totalReaches       += appReachesDagroot
            
            if cycle == numCyclesPerRun-1:
                if START_CYCLE==0:
                    initTxQueueFill = 0
                reliabilities  += [float(totalReaches)/float(totalGenerated+initTxQueueFill-txQueueFill)]
                totalGenerated  = 0
                totalReaches    = 0
            
            if cycle==0 and previousCycle and previousCycle!=numCyclesPerRun-1:
                print 'runNum({0}) in {1} is incomplete data'.format(runNum-1,dir)
                # initialize counters and skip the incomplete data
                totalGenerated  = appGenerated
                totalReaches    = appReachesDagroot
                
            previousCycle = cycle
    return elem, reliabilities

#===== battery life

def plot_batteryLife(elemName):

    dataSetDirs = []
    for dataSetDir in os.listdir(os.path.curdir):
        if os.path.isdir(dataSetDir):
            dataSetDirs += [dataSetDir]

    # verify there is some data to plot
    if dataSetDirs == []:
        print 'There are no simulation results to analyze.'
        sys.exit(1)

    batteryLives = {}
    
    # One curve is generated by each dataSetDir             
    for dataSetDir in dataSetDirs: 
        
        batteryLivesEachSet = {}

        # assume that each data file in dataSetDir uses same parameters other than a value of elemName 
        for dir in os.listdir(dataSetDir):
            
            for infilename in glob.glob(os.path.join(dataSetDir, dir,'*.dat')):
                
                elem, batteryLivesEachElem = calcBatteryLife(
                    dir                = os.path.join(dataSetDir, dir),
                    infilename         = os.path.basename(infilename),
                    elemName           = elemName
                )
                
                batteryLivesEachSet[elem] = batteryLivesEachElem
                
        batteryLives[dataSetDir] = batteryLivesEachSet
            
    if elemName == 'numMotes':
        xlabel = 'number of motes'
    elif elemName == 'pkPeriod':
        xlabel = 'packet period (s)'
    elif elemName == 'otfThreshold':
        xlabel = 'OTF threshold'
    elif elemName == 'sixtopPdrThreshold':
        xlabel = 'threshold for cell relocation'
    elif elemName == 'numChans':
        xlabel = 'number of channels'
    else:
        xlabel = elemName
    
    outfilename    = 'output_battery_{}'.format(elemName)

    # plot figure for battery life    
    genStatsVsParameterPlots(
        batteryLives,
        dirs           = dataSetDirs,
        outfilename    = outfilename,
        xlabel         = xlabel,
        ylabel         = 'battery life (day)',
    )
            
def calcBatteryLife(dir,infilename,elemName):

    CAPACITY = 2200 # in mAh
    
    infilepath     = os.path.join(dir,infilename)

    with open(infilepath,'r') as f:
        for line in f:
            
            if line.startswith('##'):
                # elem
                m = re.search(elemName+'\s+=\s+([\.0-9]+)',line)
                if m:
                    elem               = float(m.group(1))

                # slotDuration
                m = re.search('slotDuration\s+=\s+([\.0-9]+)',line)
                if m:
                    slotDuration       = float(m.group(1))

                # slotframeLength
                m = re.search('slotframeLength\s+=\s+([\.0-9]+)',line)
                if m:
                    slotframeLength    = int(m.group(1))
        
    minBatteryLives = []
    with open(infilepath,'r') as f:
        for line in f:
            
            if line.startswith('#aveChargePerCycle'):
                
                # runNum
                m      = re.search('runNum=([0-9]+)',line)
                runNum = int(m.group(1))
                                
                # maximum average charge
                m           = re.findall('([0-9]+)@([\.0-9]+)',line)
                lm          = [list(t) for t in m if int(list(t)[0]) != 0] # excluding DAG root
                maxIdCharge = max(lm, key=lambda x: float(x[1]))
                maxCurrent  = float(maxIdCharge[1])*10**(-3)/(slotDuration*slotframeLength) # convert from uC/cycle to mA
                
                # battery life
                minBatteryLives += [CAPACITY/maxCurrent/24] # mAh/mA/(h/day), in day
                
    return elem, minBatteryLives    

#===== latency

def plot_latency(elemName):
        
    dataSetDirs = []
    for dataSetDir in os.listdir(os.path.curdir):
        if os.path.isdir(dataSetDir):
            dataSetDirs += [dataSetDir]

    # verify there is some data to plot
    if dataSetDirs == []:
        print 'There are no simulation results to analyze.'
        sys.exit(1)

    latencies    = {}
        
    # One curve is generated by each dataSetDir
    for dataSetDir in dataSetDirs: 

        latenciesEachSet = {}

        # assume that each data file in dataSetDir uses same parameters other than a value of elemName
        for dir in os.listdir(dataSetDir):
            
            for infilename in glob.glob(os.path.join(dataSetDir, dir,'*.dat')):
                
                elem, latenciesEachElem = calcLatency(
                    dir                = os.path.join(dataSetDir, dir),
                    infilename         = os.path.basename(infilename),
                    elemName           = elemName
                )
                
                latenciesEachSet[elem] = latenciesEachElem
                
        latencies[dataSetDir]    = latenciesEachSet
            

    if elemName == 'numMotes':
        xlabel = 'number of motes'
    elif elemName == 'pkPeriod':
        xlabel = 'packet period (s)'
    elif elemName == 'otfThreshold':
        xlabel = 'OTF threshold'
    elif elemName == 'sixtopPdrThreshold':
        xlabel = 'threshold for cell relocation'
    elif elemName == 'numChans':
        xlabel = 'number of channels'
    else:
        xlabel = elemName
    
    outfilename    = 'output_latency_{}'.format(elemName)

    # plot figure for latency    
    genStatsVsParameterPlots(
        latencies,
        dirs           = dataSetDirs,
        outfilename    = outfilename,
        xlabel         = xlabel,
        ylabel         = 'average latency (slot)',
    )

def calcLatency(dir,infilename,elemName):

    infilepath     = os.path.join(dir,infilename)

    with open(infilepath,'r') as f:
        for line in f:
            
            if line.startswith('##'):

                # elem
                m = re.search(elemName+'\s+=\s+([\.0-9]+)',line)
                if m:
                    elem               = float(m.group(1))
                
                # numCyclesPerRun
                m = re.search('numCyclesPerRun\s+=\s+([\.0-9]+)',line)
                if m:
                    numCyclesPerRun    = int(m.group(1))

    assert numCyclesPerRun > START_CYCLE
    
    # find appReachesDagroot, aveLatency
    with open(infilepath,'r') as f:
        for line in f:
            if line.startswith('# '):
                elems                   = re.sub(' +',' ',line[2:]).split()
                numcols                 = len(elems)
                colnumappReachesDagroot = elems.index('appReachesDagroot')
                colnumaveLatency        = elems.index('aveLatency')
                colnumcycle             = elems.index('cycle')
                colnumrunNum            = elems.index('runNum')                
                break
    
    assert colnumappReachesDagroot
    assert colnumaveLatency
    assert colnumcycle
    
    # parse data
    latencies              = []
    sumaveLatency          = 0
    sumappReachesDagroot   = 0
    previousCycle          = None
    with open(infilepath,'r') as f:
        for line in f:
            if line.startswith('#') or not line.strip():
                continue
            m                          = re.search('\s+'.join(['([\.0-9]+)']*numcols),line.strip())
            appReachesDagroot          = int(m.group(colnumappReachesDagroot+1))
            aveLatency                 = float(m.group(colnumaveLatency+1))      
            cycle                      = int(m.group(colnumcycle+1))
            runNum                     = int(m.group(colnumrunNum+1))
            
            if cycle>=START_CYCLE:
                sumappReachesDagroot      += appReachesDagroot
                sumaveLatency             += aveLatency*appReachesDagroot
            
            if cycle == numCyclesPerRun-1:
                latencies             += [sumaveLatency/sumappReachesDagroot]
                sumaveLatency          = 0
                sumappReachesDagroot   = 0

            if cycle==0 and previousCycle and previousCycle!=numCyclesPerRun-1:
                print 'runNum({0}) in {1} is incomplete data'.format(runNum-1,dir)
                # initialize counters and skip incomplete data
                sumaveLatency          = appReachesDagroot
                sumappReachesDagroot   = aveLatency*appReachesDagroot
                
            previousCycle = cycle
            
    return elem, latencies

#===== one of statistics of the last cycle in output.dat

def plot_statsOfLastCycle(elemName,statsName):
    
    dataSetDirs = []
    for dataSetDir in os.listdir(os.path.curdir):
        if os.path.isdir(dataSetDir):
            dataSetDirs += [dataSetDir]
    
    stats      = {}
    
    # One curve is generated by each dataSetDir
    for dataSetDir in dataSetDirs: 

        statsEachSet = {}
        # assume that each data file in dataSetDir uses same parameters other than a value of elemName
        for dir in os.listdir(dataSetDir):
            
            for infilename in glob.glob(os.path.join(dataSetDir, dir,'*.dat')):
                
                elem, statsEachElem = calcStatsOfLastCycle(
                    dir                = os.path.join(dataSetDir, dir),
                    infilename         = os.path.basename(infilename),
                    elemName           = elemName,
                    statsName          = statsName,
                )
                
                statsEachSet[elem]    = statsEachElem
                                
        stats[dataSetDir] = statsEachSet
            

    if elemName == 'numMotes':
        xlabel = 'number of motes'
    elif elemName == 'pkPeriod':
        xlabel = 'packet period (s)'
    elif elemName == 'otfThreshold':
        xlabel = 'OTF threshold'
    elif elemName == 'sixtopPdrThreshold':
        xlabel = 'threshold for cell relocation'
    elif elemName == 'numChans':
        xlabel = 'number of channels'
    else:
        xlabel = elemName
    
    if statsName == 'numTxCells':
        ylabel = 'number of Tx cells'
    else:
        ylabel = statsName
    
    outfilename    = 'output_{}_{}'.format(statsName,elemName)
    
    # plot figure for e2e reliability   
    genStatsVsParameterPlots(
        stats,
        dirs           = dataSetDirs,
        outfilename    = outfilename,
        xlabel         = xlabel,
        ylabel         = ylabel,
    )



def calcStatsOfLastCycle(dir,infilename,elemName,statsName):
    
    infilepath     = os.path.join(dir,infilename)
    
    # find xAxis, numCyclesPerRun    
    with open(infilepath,'r') as f:
        for line in f:
            
            if line.startswith('##'):
                
                # elem
                m = re.search(elemName+'\s+=\s+([\.0-9]+)',line)
                if m:
                    elem               = float(m.group(1))
                
                # numCyclesPerRun
                m = re.search('numCyclesPerRun\s+=\s+([\.0-9]+)',line)
                if m:
                    numCyclesPerRun    = int(m.group(1))
    
    # find colnumcycle
    with open(infilepath,'r') as f:
        for line in f:
            if line.startswith('# '):
                elems                   = re.sub(' +',' ',line[2:]).split()
                numcols                 = len(elems)
                colnumstatsName         = elems.index(statsName)
                colnumcycle             = elems.index('cycle')
                colnumrunNum            = elems.index('runNum')                
                break
        
    # parse data
    stats = []
    previousCycle  = None
    with open(infilepath,'r') as f:
        for line in f:
            if line.startswith('#') or not line.strip():
                continue
            m = re.search('\s+'.join(['([\.0-9]+)']*numcols),line.strip())
            stat                = int(m.group(colnumstatsName+1))
            cycle               = int(m.group(colnumcycle+1))
            runNum              = int(m.group(colnumrunNum+1))
            if cycle == numCyclesPerRun-1:
                stats += [stat]

            if cycle==0 and previousCycle and previousCycle!=numCyclesPerRun-1:
                print 'runNum({0}) in {1} is incomplete data'.format(runNum-1,dir)
                
            previousCycle = cycle
    
    return elem, stats

#============================ plotters ========================================
 
def genStatsVsParameterPlots(vals, dirs, outfilename, xlabel, ylabel, xmin=False, xmax=False, ymin=False, ymax=False, log=False):

    # print
    print 'Generating {0}...'.format(outfilename),
    
    matplotlib.pyplot.figure()
    matplotlib.pyplot.xlabel(xlabel, fontsize='large')
    matplotlib.pyplot.ylabel(ylabel, fontsize='large')
    if log:
        matplotlib.pyplot.yscale('log')

    for dataSetDir in dirs:
        # calculate mean and confidence interval
        meanPerParameter    = {}
        confintPerParameter = {}
        for (k,v) in vals[dataSetDir].items():
            a                        = 1.0*numpy.array(v)
            n                        = len(a)
            se                       = scipy.stats.sem(a)
            m                        = numpy.mean(a)
            confint                  = se * scipy.stats.t._ppf((1+CONFINT)/2., n-1)
            meanPerParameter[k]      = m
            confintPerParameter[k]   = confint
    
        # plot
        x         = sorted(meanPerParameter.keys())
        y         = [meanPerParameter[k] for k in x]
        yerr      = [confintPerParameter[k] for k in x]
        
        if dataSetDir == 'tx-housekeeping':
            index = 0
        elif dataSetDir == 'rx-housekeeping':
            index = 1
        elif dataSetDir == 'tx-rx-housekeeping':
            index = 2
        elif dataSetDir == 'no housekeeping':
            index = 3
        elif dataSetDir == 'no interference':
            index = 4

        matplotlib.pyplot.errorbar(
            x        = x,
            y        = y,
            yerr     = yerr,
            color    = COLORS[index],
            ls       = LINESTYLE[index],
            ecolor   = ECOLORS[index],
            label    = dataSetDir
        )
        
        datafile=open(outfilename+'.dat', "a")
        print >> datafile,dataSetDir
        print >> datafile,xlabel,x
        print >> datafile,ylabel,y
        #print >> datafile,'conf. inverval',yerr
    
    matplotlib.pyplot.legend(prop={'size':12},loc=0)
    if xmin:
        matplotlib.pyplot.xlim(xmin=xmin)
    if xmax:
        matplotlib.pyplot.xlim(xmax=xmax)
    if ymin:
        matplotlib.pyplot.ylim(ymin=ymin)
    if ymax:
        matplotlib.pyplot.ylim(ymax=ymax)
    matplotlib.pyplot.savefig(outfilename + '.png')
    matplotlib.pyplot.savefig(outfilename + '.eps')    
    matplotlib.pyplot.close('all')
    
    # print
    print 'done.'

#============================ main ============================================
def main():
    
    # parse CLI option
    options      = parseCliOptions()
    elemName     = options['elemName']
    statsName    = options['statsName']

    # stats vs elemName
    plot_statsOfLastCycle(elemName, statsName)

    # reliability vs elemName
    plot_reliability(elemName)
    
    # latency vs elemName
    plot_latency(elemName)
    
    # battery life vs elemName
    plot_batteryLife(elemName)
        
if __name__=="__main__":
    main()