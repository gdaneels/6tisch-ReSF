#!/usr/bin/python
'''
\brief Entry point to start a simulation.

A number of command-line parameters are available to modify the simulation
settings. Use '--help' for a list of them.

\author Thomas Watteyne <watteyne@eecs.berkeley.edu>
\author Kazushi Muraoka <k-muraoka@eecs.berkeley.edu>
\author Nicola Accettura <nicola.accettura@eecs.berkeley.edu>
\author Xavier Vilajosana <xvilajosana@eecs.berkeley.edu>
'''

#============================ adjust path =====================================

import os
import sys
if __name__=='__main__':
    here = sys.path[0]
    sys.path.insert(0, os.path.join(here, '..'))

#============================ logging =========================================

import logging
class NullHandler(logging.Handler):
    def emit(self, record):
        pass
log = logging.getLogger('BatchSim')
log.setLevel(logging.ERROR)
log.addHandler(NullHandler())

#============================ imports =========================================

import time
import itertools
import logging.config
import argparse
import threading
import datetime
import traceback
from collections import OrderedDict


from SimEngine     import SimEngine,   \
                          SimSettings, \
                          SimStats
from SimGui        import SimGui

#============================ defines =========================================

#============================ helpers =========================================

def parseCliOptions():

    parser = argparse.ArgumentParser()
    # sim
    parser.add_argument('--gui',
        dest       = 'gui',
        action     = 'store_true',
        default    = False,
        help       = '[sim] Display the GUI.',
    )
    parser.add_argument( '--cpuID',
        dest       = 'cpuID',
        type       = int,
        default    = None,
        help       = '[sim] id of the CPU running this simulation (for batch).',
    )
    parser.add_argument( '--numRuns',
        dest       = 'numRuns',
        type       = int,
        default    = 1,
        help       = '[sim] Number of simulation runs.',
    )
    parser.add_argument( '--numCyclesPerRun',
        dest       = 'numCyclesPerRun',
        type       = int,
        default    = 700,
        help       = '[simulation] Duration of a run, in slotframes.',
    )
    parser.add_argument('--simDataDir',
        dest       = 'simDataDir',
        type       = str,
        default    = 'simData',
        help       = '[simulation] Simulation log directory.',
    )
    # topology
    parser.add_argument( '--numMotes',
        dest       = 'numMotes',
        nargs      = '+',
        type       = int,
        default    = [25],
        help       = '[topology] Number of simulated motes.',
    )
    parser.add_argument( '--squareSide',
        dest       = 'squareSide',
        type       = float,
        default    = 2.000,
        help       = '[topology] Side of the deployment area (km).',
    )
    # app
    parser.add_argument( '--pkPeriod',
        dest       = 'pkPeriod',
        nargs      = '+',
        type       = float,
        default    = 10,
        help       = '[app] Average period between two data packets (s).',
    )
    parser.add_argument( '--pkPeriodStd',
        dest       = 'pkPeriodStd',
        nargs      = '+',
        type       = float,
        default    = 3.3,
        help       = '[app] Std of packet period.',
    )
    parser.add_argument( '--pkPeriodVar',
        dest       = 'pkPeriodVar',
        type       = float,
        default    = 0.05,
        help       = '[app] Variability of pkPeriod (0.00-1.00).',
    )
    parser.add_argument( '--burstTimestamp',
        dest       = 'burstTimestamp',
        nargs      = '+',
        type       = float,
        default    = 20,
        help       = '[app] Timestamp when the burst happens (s).',
    )
    parser.add_argument( '--numPacketsBurst',
        dest       = 'numPacketsBurst',
        nargs      = '+',
        type       = int,
        default    = 5,
        help       = '[app] Number of packets in a burst, per node.',
    )
    # rpl
    parser.add_argument( '--dioPeriod',
        dest       = 'dioPeriod',
        type       = float,
        default    = 1.0,
        help       = '[rpl] DIO period (s).',
    )
    # otf
    parser.add_argument( '--otfThreshold',
        dest       = 'otfThreshold',
        nargs      = '+',
        type       = int,
        default    = [0],
        help       = '[otf] OTF threshold (cells).',
    )
    parser.add_argument( '--otfHousekeepingPeriod',
        dest       = 'otfHousekeepingPeriod',
        type       = float,
        default    = 10.0,
        help       = '[otf] OTF housekeeping period (s).',
    )
    # sixtop
    parser.add_argument( '--sixtopHousekeepingPeriod',
        dest       = 'sixtopHousekeepingPeriod',
        type       = float,
        default    = 1.0,
        help       = '[6top] 6top housekeeping period (s).',
    )
    parser.add_argument( '--sixtopPdrThreshold',
        dest       = 'sixtopPdrThreshold',
        type       = float,
        default    = 1.5,
        help       = '[6top] 6top PDR threshold.',
    )
    parser.add_argument('--sixtopNoHousekeeping',
        dest       = 'sixtopNoHousekeeping',
        nargs      = '+',
        type       = int,
        default    = 1,
        help       = '[6top] 1 to disable 6top housekeeping.',
    )
    parser.add_argument('--sixtopNoRemoveWorstCell',
        dest       = 'sixtopNoRemoveWorstCell',
        nargs      = '+',
        type       = int,
        default    = 0,
        help       = '[6top] 1 to remove random cell, not worst.',
    )
    parser.add_argument('--sixtopInTXRX',
        dest       = 'sixtopInTXRX',
        type       = bool,
        default    = True,
        help       = '[6top] Send 6P messages in TX/RX slots.',
    )
    parser.add_argument('--sixtopInSHARED',
        dest       = 'sixtopInSHARED',
        type       = bool,
        default    = True,
        help       = '[6top] Send 6P messages in SHARED slots.',
    )
    # tsch
    parser.add_argument( '--slotDuration',
        dest       = 'slotDuration',
        type       = float,
        default    = 0.010,
        help       = '[tsch] Duration of a timeslot (s).',
    )
    parser.add_argument( '--slotframeLength',
        dest       = 'slotframeLength',
        type       = int,
        default    = 101,
        help       = '[tsch] Number of timeslots in a slotframe.',
    )
    parser.add_argument( '--switchFrameLengths',
        dest       = 'switchFrameLengths',
        type       = bool,
        default    = False,
        help       = '[tsch] Enable/disable switching frame lengths for normal and shared slotframes.',
    )
    parser.add_argument( '--sharedframeLength',
        dest       = 'sharedframeLength',
        type       = int,
        default    = 10,
        help       = '[tsch] Number of timeslots in a shared slot slotframe.',
    )
    parser.add_argument( '--sharedframeInterval',
        dest       = 'sharedframeInterval',
        type       = int,
        default    = 4,
        help       = '[tsch] Number of normal slotframes between every shared slotframe.',
    )
    # phy
    parser.add_argument( '--numChans',
        dest       = 'numChans',
        type       = int,
        default    = 16,
        help       = '[phy] Number of frequency channels.',
    )
    parser.add_argument( '--minRssi',
        dest       = 'minRssi',
        type       = int,
        default    = -97,
        help       = '[phy] Mininum RSSI with positive PDR (dBm).',
    )
    parser.add_argument('--noInterference',
        dest       = 'noInterference',
        nargs      = '+',
        type       = int,
        default    = 0,
        help       = '[phy] Disable interference model.',
    )
    parser.add_argument('--llsf',
        dest       = 'llsf',
        nargs      = '+',
        type       = bool,
        default    = False,
        help       = '[otf] Enable the LLSF schedule.',
    )
    parser.add_argument('--sf',
        dest       = 'sf',
        type       = str,
        default    = 'recurrent_chang',
        help       = '[otf] Enable the correct scheduling function.',
    )
    parser.add_argument('--topologyMode',
        dest       = 'topologyMode',
        nargs      = '+',
        type       = str,
        default    = 'mesh',
        help       = 'The structure of the topology.',
    )
    parser.add_argument('--bootDelay',
        dest       = 'bootDelay',
        type       = int,
        default    = 10,
        help       = 'The boot delay (seconds) between two motes booting.',
    )
    parser.add_argument('--delayAfterBooting',
        dest       = 'delayAfterBooting',
        type       = int,
        default    = 200,
        help       = 'The delay (seconds) between when the last node has succeeded his LLSF requests and when the first mote starts sending.',
    )
    parser.add_argument('--randomSeeds',
        dest       = 'randomSeeds',
        type       = str,
        default    = '[]',
        help       = '[randomseeds] The random seeds for the topology and propagation.',
    )
    parser.add_argument('--numCPUs',
        dest       = 'numCPUs',
        type       = int,
        default    = '1',
        help       = '[numCPUs] The total number of CPUs that is used.',
    )
    parser.add_argument('--perfectLinks',
        dest       = 'perfectLinks',
        type       = int,
        default    = 0,
        help       = '[perfectLinks] ETX = 1 or not.',
    )
    parser.add_argument('--sampledTraffic',
        dest       = 'sampledTraffic',
        type       = int,
        default    = 1,
        help       = '[sampledTraffic] Sample traffic from a normal distribution with mean pkPeriod and stdDev=pkPeriod/3.',
    )
    parser.add_argument('--dynamicNetwork',
        dest       = 'dynamicNetwork',
        type       = int,
        default    = 0,
        help       = '[dynamicNetwork] There is a probability that nodes change their packet generation frequency.',
    )
    parser.add_argument('--changeFreqProbability',
        dest       = 'changeFreqProbability',
        type       = float,
        default    = 0.8,
        help       = '[changeFreqProbability] The probability that a node should change its packet generation frequency.',
    )
    parser.add_argument('--collisionRateThreshold',
        dest       = 'collisionRateThreshold',
        type       = float,
        default    = 0.05,
        help       = '[collisionRateThreshold] The collision rate threshold.',
    )
    parser.add_argument('--reservationBuffer',
        dest       = 'reservationBuffer',
        type       = int,
        default    = 64,
        help       = '[reservationBuffer] The number of cells out of which ReSF can choose the cells with the lowest collision rate.',
    )
    options        = parser.parse_args()

    return options.__dict__

def printOrLog(simParam,output):
    if simParam['cpuID']!=None:
        with open('cpu{0}.templog'.format(simParam['cpuID']),'w') as f:
            f.write(output)
    else:
        print output

def runSims(options):

    # record simulation start time
    simStartTime   = time.time()

    successRuns = OrderedDict()

    # compute all the simulation parameter combinations
    combinationKeys     = sorted([k for (k,v) in options.items() if type(v)==list])
    simParams           = []
    for p in itertools.product(*[options[k] for k in combinationKeys]):
        simParam = OrderedDict()
        for (k,v) in zip(combinationKeys,p):
            simParam[k] = v
        for (k,v) in options.items():
            if k not in simParam:
                simParam[k] = v
        simParams      += [simParam]

    # run a simulation for each set of simParams
    for (simParamNum,simParam) in enumerate(simParams):

        # record run start time
        runStartTime = time.time()
        # succesRun = False
        # run the simulation runs
        for runNum in xrange(simParam['numRuns']):
            successRuns[runNum] = False

            # print
            output  = 'parameters {0}/{1}, run {2}/{3}'.format(
               simParamNum+1,
               len(simParams),
               runNum+1,
               simParam['numRuns']
            )
            printOrLog(simParam,output)

            # create singletons
            settings         = SimSettings.SimSettings(**simParam)
            settings.setStartTime(runStartTime)
            settings.setCombinationKeys(combinationKeys)
            simengine        = SimEngine.SimEngine(runNum)
            simstats         = SimStats.SimStats(runNum)

            # start simulation run
            simengine.start()

            # wait for simulation run to end
            simengine.join()

            # destroy singletons
            simstats.destroy()

            successRuns[runNum] = (simengine.getSuccessRun(), simengine.randomSeed, options)

            simengine.destroy()
            settings.destroy()

        # print
        output  = 'simulation ended after {0:.0f}s.'.format(time.time()-simStartTime)
        printOrLog(simParam,output)

        for runNum, tupleResult  in successRuns.iteritems():
            if not tupleResult[0]:
                errorfile = 'errorfile.plt'
                msg = '%s: Experiment run %d went wrong, with randomSeed %s, options: %s.' % (str(datetime.datetime.now()), runNum, str(tupleResult[1]), str(tupleResult[2]))
                with open(errorfile, 'a') as outfile:
                    outfile.write(msg)
                    outfile.write('\r\n')
                raise BaseException(msg)


#============================ main ============================================

def main():
    # try:
    # initialize logging
    logging.config.fileConfig('logging.conf')

    # parse CLI options
    options        = parseCliOptions()

    if options['gui']:
        # create the GUI
        gui        = SimGui.SimGui()

        # run simulations (in separate thread)
        simThread  = threading.Thread(target=runSims,args=(options,))
        simThread.start()

        # start GUI's mainloop (in main thread)
        gui.mainloop()
    else:
        # run the simulations
        runSims(options)
    # except:
    #     tb = traceback.format_exc()
    #     print tb


if __name__=="__main__":
    main()
