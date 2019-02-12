#!/usr/bin/python
'''
\brief Collects and logs statistics about the ongoing simulation.

\author Thomas Watteyne <watteyne@eecs.berkeley.edu>
\author Kazushi Muraoka <k-muraoka@eecs.berkeley.edu>
\author Nicola Accettura <nicola.accettura@eecs.berkeley.edu>
\author Xavier Vilajosana <xvilajosana@eecs.berkeley.edu>
'''

#============================ logging =========================================

import logging
class NullHandler(logging.Handler):
    def emit(self, record):
        pass
log = logging.getLogger('SimStats')
log.setLevel(logging.ERROR)
log.addHandler(NullHandler())

#============================ imports =========================================

import SimEngine
import SimSettings
import numpy as np
import os
import json
# import matplotlib.pyplot as plt

from collections import OrderedDict

#============================ defines =========================================

#============================ body ============================================

class SimStats(object):

    #===== start singleton
    _instance      = None
    _init          = False

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super(SimStats,cls).__new__(cls, *args, **kwargs)
        return cls._instance
    #===== end singleton

    def __init__(self,runNum):

        #===== start singleton
        if self._init:
            return
        self._init = True
        #===== end singleton

        # store params
        self.runNum                         = runNum

        # local variables
        self.engine                         = SimEngine.SimEngine()
        self.settings                       = SimSettings.SimSettings()

        # stats
        self.stats                          = OrderedDict()
        self.columnNames                    = []

        self.batteryPerMote                 = OrderedDict()
        self.txActives                      = OrderedDict()
        self.rxActives                      = OrderedDict()

        # start file
        if self.runNum==0:
            self._fileWriteHeader()

        # schedule actions
        self.engine.scheduleAtStart(
            cb          = self._actionStart,
        )

        # gdaneels, schedule at last asn of cycle
        if self.settings.switchFrameLengths:
            self.engine.scheduleAtAsn(
                asn         = self.engine.getEndASN(),
                cb          = self._actionEndCycle,
                uniqueTag   = (None,'_actionEndCycle'),
                priority    = 10,
            )
        else:
            self.engine.scheduleAtAsn(
                asn         = self.engine.getAsn()+self.settings.slotframeLength-1,
                cb          = self._actionEndCycle,
                uniqueTag   = (None,'_actionEndCycle'),
                priority    = 10,
            )
        self.engine.scheduleAtEnd(
            cb          = self._actionEnd,
        )

    def destroy(self):
        # destroy my own instance
        self._instance                      = None
        self._init                          = False

    #======================== private =========================================

    def _actionStart(self):
        '''Called once at beginning of the simulation.'''
        pass

    def _actionEndCycle(self):
        '''Called at each end of cycle.'''
        cycle = int(self.engine.getAsn()/self.settings.slotframeLength)
        if self.settings.switchFrameLengths:
            cycle = self.engine.getCycle()

        # print
        if self.settings.cpuID==None:
            print('   cycle: {0}/{1}'.format(cycle,self.settings.numCyclesPerRun-1))

        # write statistics to output file
        self._fileWriteStats(
            dict(
                {
                    'runNum':              self.runNum,
                    'cycle':               cycle,
                }.items() +
                self._collectSumMoteStats().items()  +
                self._collectScheduleStats().items()
            )
        )

        self.engine.parentConvergence[self.engine.asn] = self.checkParentConvergence()
        self.engine.childConvergence[self.engine.asn] = self.checkChildConvergence()
        if self.settings.sf == 'recurrent_chang':
            self.engine.recurrentConvergence[self.engine.asn] = self.checkRecurrentConvergence()
            self.engine.reachedRootConvergence[self.engine.asn] = self.checkRecurrenceReachedRoot()
            self.engine.reachedNormalRootConvergence[self.engine.asn] = self.checkNormalRecurrenceReachedRoot()

        for mote in self.engine.motes:
            moteStats        = mote.getMoteStats()
            if mote.id not in self.batteryPerMote:
                self.batteryPerMote[mote.id] = []
            self.batteryPerMote[mote.id].append(moteStats['chargeConsumed'])

        if self.settings.switchFrameLengths:
            print 'SimStats: %d' % self.engine.getEndASN()
            self.engine.scheduleAtAsn(
                asn         = self.engine.getNextCycleEndASN(),
                cb          = self._actionEndCycle,
                uniqueTag   = (None,'_actionEndCycle'),
                priority    = 10,
            )
        else:
            # schedule next statistics collection
            self.engine.scheduleAtAsn(
                asn         = self.engine.getAsn()+self.settings.slotframeLength,
                cb          = self._actionEndCycle,
                uniqueTag   = (None,'_actionEndCycle'),
                priority    = 10,
            )

    def _actionEnd(self):
        '''Called once at end of the simulation.'''
        self._fileWriteTopology()
        self.writeReport()

    #=== collecting statistics

    def _collectSumMoteStats(self):
        returnVal = OrderedDict()

        for mote in self.engine.motes:
            moteStats        = mote.getMoteStats()
            if not returnVal:
                returnVal    = moteStats
            else:
                for k in returnVal.keys():
                    returnVal[k] += moteStats[k]

        returnVal['droppedAll'] = returnVal['droppedQueueFull'] + returnVal['droppedNoRoute'] + returnVal['droppedNoTxCells'] + returnVal['droppedMacRetries'] + returnVal['droppedQueueFullRelayed'] + returnVal['droppedNoRouteRelayed'] + returnVal['droppedNoTxCellsRelayed']
        returnVal['topGenerated'] = returnVal['topGeneratedAdd'] + returnVal['topGeneratedResponse'] + returnVal['topGeneratedDelete'] + returnVal['topGeneratedDeleteResponse']
        returnVal['topSuccessGenerated'] = returnVal['topSuccessGeneratedAdd'] + returnVal['topSuccessGeneratedResponse'] + returnVal['topSuccessGeneratedDelete'] + returnVal['topSuccessGeneratedDeleteResponse']

        return returnVal

    def _collectScheduleStats(self):

        # compute the number of schedule collisions

        # Note that this cannot count past schedule collisions which have been relocated by 6top
        # as this is called at the end of cycle
        scheduleCollisions = 0
        txCells = []
        for mote in self.engine.motes:
            for (ts,cell) in mote.schedule.items():
                (ts,ch) = (ts,cell['ch'])
                if cell['dir']==mote.DIR_TX:
                    if (ts,ch) in txCells:
                        scheduleCollisions += 1
                    else:
                        txCells += [(ts,ch)]

        # collect collided links
        txLinks = OrderedDict()
        for mote in self.engine.motes:
            for (ts,cell) in mote.schedule.items():
                if cell['dir']==mote.DIR_TX:
                    (ts,ch) = (ts,cell['ch'])
                    (tx,rx) = (mote,cell['neighbor'])
                    if (ts,ch) in txLinks:
                        txLinks[(ts,ch)] += [(tx,rx)]
                    else:
                        txLinks[(ts,ch)]  = [(tx,rx)]

        collidedLinks = [txLinks[(ts,ch)] for (ts,ch) in txLinks if len(txLinks[(ts,ch)])>=2]

        # compute the number of Tx in schedule collision cells
        collidedTxs = 0
        for links in collidedLinks:
            collidedTxs += len(links)

        # compute the number of effective collided Tx
        effectiveCollidedTxs = 0
        insufficientLength   = 0
        for links in collidedLinks:
            for (tx1,rx1) in links:
                for (tx2,rx2) in links:
                    if tx1!=tx2 and rx1!=rx2:
                        # check whether interference from tx1 to rx2 is effective
                        if tx1 == rx2 or tx1.getRSSI(rx2) > rx2.minRssi:
                            # self.engine.totalTXCollisions += 1
                            # cycleOfCollision = self.engine.asn % self.settings.slotframeLength
                            # self.engine.TXCollisionsCycles.append(cycleOfCollision)
                            # print 'Collision at ASN %d between tx1 %d and rx2 %d' % (self.engine.asn, tx1.id, rx2.id)
                            effectiveCollidedTxs += 1


        return {'scheduleCollisions':scheduleCollisions, 'collidedTxs': collidedTxs, 'effectiveCollidedTxs': effectiveCollidedTxs}

    #=== writing to file

    def _fileWriteHeader(self):
        output          = []
        output         += ['## {0} = {1}'.format(k,v) for (k,v) in self.settings.__dict__.items() if not k.startswith('_')]
        output         += ['\n']
        output          = '\n'.join(output)

        with open(self.settings.getOutputFile(),'w') as f:
            f.write(output)

    def _fileWriteStats(self,stats):
        output          = []

        # columnNames
        if not self.columnNames:
            self.columnNames = sorted(stats.keys())
            output     += ['\n# '+' '.join(self.columnNames)]

        # dataline
        formatString    = ' '.join(['{{{0}:>{1}}}'.format(i,len(k)) for (i,k) in enumerate(self.columnNames)])
        formatString   += '\n'

        vals = []
        for k in self.columnNames:
            if type(stats[k])==float:
                vals += ['{0:.3f}'.format(stats[k])]
            else:
                vals += [stats[k]]

        output += ['  '+formatString.format(*tuple(vals))]

        # write to file
        with open(self.settings.getOutputFile(),'a') as f:
            f.write('\n'.join(output))

    def _fileWriteTopology(self):
        output  = []
        output += [
            '#pos runNum={0} {1}'.format(
                self.runNum,
                ' '.join(['{0}@({1:.5f},{2:.5f})@{3}'.format(mote.id,mote.x,mote.y,mote.rank) for mote in self.engine.motes])
            )
        ]
        links = OrderedDict()
        for m in self.engine.motes:
            for n in self.engine.motes:
                if m==n:
                    continue
                if (n,m) in links:
                    continue
                try:
                    links[(m,n)] = (m.getRSSI(n),m.getPDR(n))
                except KeyError:
                    pass
        output += [
            '#links runNum={0} {1}'.format(
                self.runNum,
                ' '.join(['{0}-{1}@{2:.0f}dBm@{3:.3f}'.format(moteA.id,moteB.id,rssi,pdr) for ((moteA,moteB),(rssi,pdr)) in links.items()])
            )
        ]
        output += [
            '#aveChargePerCycle runNum={0} {1}'.format(
                self.runNum,
                ' '.join(['{0}@{1:.2f}'.format(mote.id,mote.getMoteStats()['chargeConsumed']/self.settings.numCyclesPerRun) for mote in self.engine.motes])
            )
        ]
        output  = '\n'.join(output)

        with open(self.settings.getOutputFile(),'a') as f:
            f.write(output)

    def checkParentConvergence(self):
        countConverged = 0
        for i in range(len(self.engine.motes)):
            if self.engine.motes[i] != 0 and self.engine.motes[i].preferredParent is not None: # the mote should not have a preferred parent for this
                if len(self.engine.motes[i].getTxCellsNeighborNoRecurrent(self.engine.motes[i].preferredParent)) > 0:
                    countConverged += 1
        return countConverged

    def checkChildConvergence(self):
        countConverged = 0
        parentsOfMotes = OrderedDict() # parent to children map
        parentsOfMotes['noParent'] = [] # no parent yet
        # 1) look for parents first
        for i in range(len(self.engine.motes)):
            if self.engine.motes[i].preferredParent is None: # if the mote does not have pref parent yet
                parentsOfMotes['noParent'].append(self.engine.motes[i])
            else:
                if self.engine.motes[i].preferredParent.id not in parentsOfMotes:
                    parentsOfMotes[self.engine.motes[i].preferredParent.id] = []
                parentsOfMotes[self.engine.motes[i].preferredParent.id].append(self.engine.motes[i])

        # 2) look for convergence
        for i in range(len(self.engine.motes)):
            if self.engine.motes[i].id in parentsOfMotes:
                for moteID in parentsOfMotes[self.engine.motes[i].id]:
                    if len(self.engine.motes[i].getTxCellsNeighborNoRecurrent(moteID)) > 0:
                        countConverged += 1
        return countConverged

    def isTagInTuples(self, tag, tuples):
        if tuples == None or len(tuples) == 0:
            return False
        for t in tuples:
            if t[7] == tag:
                return True
        return False

    def searchForTag(self, tag, mote, originator = False):
        if mote == None:
            # print 'For tag %s, mote None.'
            return False
        if originator and (self.isTagInTuples(tag, mote.LLSFReservationsTXTuples) and not self.isTagInTuples(tag, mote.LLSFReservationsRXTuples)): # originator mote
            # print 'For tag %s, starting from mote %d.' % (tag, mote.id)
            return self.searchForTag(tag, mote.preferredParent)
        elif not originator and (self.isTagInTuples(tag, mote.LLSFReservationsRXTuples) and self.isTagInTuples(tag, mote.LLSFReservationsTXTuples)): # in between mote
            # print 'For tag %s, reached mote %d.' % (tag, mote.preferredParent.id)
            return self.searchForTag(tag, mote.preferredParent)
        elif mote.dagRoot and (self.isTagInTuples(tag, mote.LLSFReservationsRXTuples) and not self.isTagInTuples(tag, mote.LLSFReservationsTXTuples)): # root
            # print 'For tag %s, reached dagroot.' % tag
            return True

        # otherwise return False
        return False

    def printOutRootReached(self):
        print self.engine.motes[0].id
        # print self.engine.motes[0].LLSFReservationsRXTuples
        # print len(self.engine.motes[0].LLSFReservationsRXTuples)
        tags = []
        for t in self.engine.motes[0].LLSFReservationsRXTuples:
            if t[7] not in tags:
                tags.append(t[7])
        print tags
        print len(tags)

        toHaveTags = []
        for i in range(len(self.engine.motes)):
            if self.engine.motes[i].id in self.engine.sendDataMotes:
                tag = self.engine.motes[i].tagUniqueID
                toHaveTags.append(tag)

        diff = set(toHaveTags) - set(tags)
        print 'Diff: %s' % (str(diff))

    def getRootReached(self):
        tags = []
        for t in self.engine.motes[0].LLSFReservationsRXTuples:
            if t[7] not in tags:
                tags.append(t[7])
        return tags, len(tags)


    def getTotalRootReachedList(self):
        ''' Get the list of tracks unique IDs that reached the root, during the whole experiment. '''
        return self.engine.successLLSFMotes

    def getTotalRootReached(self):
        ''' Get the total number of tracks that reached the root, during the whole experiment. '''
        return len(set(self.engine.successLLSFMotes))

    def getTotalChangedRootReached(self):
        ''' Get the total number of updated tracks that reached the root, during the whole experiment. '''
        count = 0
        for uniqueID in self.engine.successLLSFMotes:
            if '_c' in uniqueID:
                count += 1
        return count

    def checkRecurrenceReachedRoot(self):
        countConverged = 0
        for i in range(len(self.engine.motes)):
            if self.engine.motes[i].id in self.engine.sendDataMotes:
                tag = self.engine.motes[i].tagUniqueID
                for t in self.engine.motes[0].LLSFReservationsRXTuples:
                    if tag == t[7]:
                        countConverged += 1
                        break
        return countConverged

    ''' Check for when the normal recurrent reservation reached the root. '''
    def checkNormalRecurrenceReachedRoot(self):
        countConverged = 0
        for i in range(len(self.engine.motes)):
            if self.engine.motes[i].id in self.engine.sendDataMotes:
                tag = self.engine.motes[i].tagUniqueID
                if tag is not None and '_c' not in tag:
                    for t in self.engine.motes[0].LLSFReservationsRXTuples:
                        if tag == t[7]:
                            countConverged += 1
                            break
                else:
                    break
        return countConverged

    def checkRecurrentConvergence(self):
        countConverged = 0
        for i in range(len(self.engine.motes)):
            if self.engine.motes[i].id in self.engine.sendDataMotes:
                tag = self.engine.motes[i].tagUniqueID
                if self.searchForTag(tag, self.engine.motes[i], True):
                    countConverged += 1
        return countConverged

    # def createDelayBinsPlot(self, jsonFile):
    #     data = None
    #     with open(jsonFile) as data_file:
    #         data = json.load(data_file)
    #
    #     for mote in data['latency']['delayPerSource']:
    #         title = 'Binned delays for mote %s' % mote
    #         # plt.figure(1)
    #         nrBins = max(data['latency']['delayPerSource'][mote])
    #         plt.hist(data['latency']['delayPerSource'][mote], bins=nrBins)
    #         plt.title(title)
    #         plt.xlabel("Latency")
    #         plt.ylabel("Frequency")
    #
    #         dirplot = 'plots_freq/%s' % data['exp_date']
    #         if not os.path.exists(dirplot):
    #             os.makedirs(dirplot)
    #         titleExt = '%s/freq_bins_mote_%s.png' % (dirplot, mote)
    #         plt.savefig(titleExt)
    #
    #         plt.clf()


    def getHopCount(self, mote, count):
        count += 1
        if mote.id == 0:
            return count
        return self.getHopCount(mote.preferredParent, count)

    def writeReport(self):
        toJSON = OrderedDict()
        print '\r\n'
        print '----------------------- REPORT -----------------------'
        print '\r\n'
        import time
        ts = time.time()
        import datetime
        strdate = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d-%H-%M-%S')

        toJSON['randomSeed'] = self.engine.randomSeed
        print 'Random seed: %.5f' % self.engine.randomSeed
        toJSON['sf'] = self.settings.sf
        print 'Scheduling function: %s' % self.settings.sf
        toJSON['numMotes'] = self.settings.numMotes
        print '# motes: %s' % self.settings.numMotes
        toJSON['exp_date'] = strdate
        print 'exp_date: %s' % strdate

        if self.settings.sf == 'recurrent_chang':
            toJSON['numUpdatedReservations'] = len(self.engine.changeReservationTuples.keys())
            print 'Number of updated reservations: %d' % toJSON['numUpdatedReservations']
            toJSON['updatedReservations'] = self.engine.changeReservationTuples
            print 'Updated reservations: %s' % str(toJSON['updatedReservations'])

        toJSON['latency'] = OrderedDict()
        # write latency per source
        avgLatencyList = []
        stdLatencyList = []
        minAvgLatency = 10000000
        maxAvgLatency = 0
        minStdLatency = 10000000
        maxStdLatency = 0
        toJSON['latency']['delayPerSource'] = OrderedDict()
        allLatencies = []
        for mote in self.engine.delayPerSource:
            allLatencies = allLatencies + self.engine.delayPerSource[mote][:]
            avg = np.mean(self.engine.delayPerSource[mote]) # avg delay for this source
            print 'Mote %d: %s' % (mote, self.engine.delayPerSource[mote])
            std = np.std(self.engine.delayPerSource[mote]) # the std of the latencies for this source
            avgLatencyList.append(avg)
            stdLatencyList.append(std)
            if avg < minAvgLatency: # set min
                minAvgLatency = avg
            if std < minStdLatency:
                minStdLatency = std
            if avg > maxAvgLatency: # set max
                maxAvgLatency = avg
            if std > maxStdLatency:
                maxStdLatency = std
            toJSON['latency']['delayPerSource'][mote] = self.engine.delayPerSource[mote]
        avgLatencies = np.mean(allLatencies)
        stdLatencies = np.std(allLatencies)
        avgAvgLatency = np.mean(avgLatencyList)
        avgStdLatency = np.mean(stdLatencyList)
        print 'Average of latency over all motes: %.4f' % avgLatencies
        toJSON['latency']['avgLatencies'] = avgLatencies
        print 'Standard deviation of latency over all motes: %.4f' % stdLatencies
        toJSON['latency']['stdLatencies'] = stdLatencies
        print 'Average of all average latencies per source: %.4f' % avgAvgLatency
        toJSON['latency']['avgAvgLatencies'] = avgAvgLatency
        print 'Minimum average latency per source: %.4f' % minAvgLatency
        toJSON['latency']['minAvgLatency'] = minAvgLatency
        print 'Maximum average latency per source: %.4f' % maxAvgLatency
        toJSON['latency']['maxAvgLatency'] = maxAvgLatency
        print 'Average of all stds of all average latencies per source: %.4f' % avgStdLatency
        toJSON['latency']['avgStdLatency'] = avgStdLatency
        print 'Minimum std latency per source: %.4f' % minStdLatency
        toJSON['latency']['minStdLatency'] = minStdLatency
        print 'Maximum std latency per source: %.4f' % maxStdLatency
        toJSON['latency']['maxStdLatency'] = maxStdLatency


        toJSON['convergence'] = OrderedDict()
        print '\r\n'
        lastChangeASN = None
        lastValue = None
        for asn, val in self.engine.parentConvergence.iteritems():
            if val != lastValue:
                lastValue = val
                lastChangeASN = asn
        # print 'First ADD 6top Message for Parent Convergence at ASN %d.' (firstMsgASN)
        cycle = -1
        if lastChangeASN != None:
            cycle = int(lastChangeASN / self.settings.slotframeLength)
        print 'Last Parent Convergence Change at ASN %s (cycle %s) to value: %s.' % (str(lastChangeASN), str(cycle), str(lastValue))
        toJSON['convergence']['lastParentConvergenceChangeASN'] = lastChangeASN
        toJSON['convergence']['lastParentConvergenceChangeValue'] = lastValue

        lastChangeASN = None
        lastValue = None
        for asn, val in self.engine.childConvergence.iteritems():
            if val != lastValue:
                lastValue = val
                lastChangeASN = asn
        cycle = -1
        if lastChangeASN != None:
            cycle = int(lastChangeASN / self.settings.slotframeLength)
        print 'Last Child Convergence Change at ASN %s (cycle %s) to value: %s.' % (str(lastChangeASN), str(cycle), str(lastValue))
        toJSON['convergence']['lastChildConvergenceChangeASN'] = lastChangeASN
        toJSON['convergence']['lastChildConvergenceChangeValue'] = lastValue

        if self.settings.sf == 'recurrent_chang':
            ''' This first metric shows the number of tracks from node to root that are complete by actually following this path from node to root. '''
            lastChangeASN = None
            lastValue = None
            for asn, val in self.engine.recurrentConvergence.iteritems():
                if val != lastValue:
                    lastValue = val
                    lastChangeASN = asn
            cycle = -1
            if lastChangeASN != None:
                cycle = int(lastChangeASN / self.settings.slotframeLength)
            print 'Last Recurrent Convergence Change at ASN %s (cycle %s) to value: %s.' % (str(lastChangeASN), str(cycle), str(lastValue))
            print '\r\n'
            toJSON['convergence']['lastRecurrentConvergenceChangeASN'] = lastChangeASN
            toJSON['convergence']['lastRecurrentConvergenceChangeValue'] = lastValue

            ''' This metric shows the number and asn of ONLY NORMAL RECURRENT tracks or unique IDS that actually are in the root's received reservation list at the last change of this reservation list. '''
            lastChangeASN = None
            lastValue = None
            for asn, val in self.engine.reachedNormalRootConvergence.iteritems():
                if val == len(self.engine.sendDataMotes):
                    lastValue = val
                    lastChangeASN = asn
                    break # once you've found this moment, stop
            cycle = -1
            if lastChangeASN != None:
                cycle = int(lastChangeASN / self.settings.slotframeLength)
            print 'Last NORMAL Recurrent Reached root at ASN %s (cycle %s) to value: %s.' % (str(lastChangeASN), str(cycle), str(lastValue))
            toJSON['convergence']['lastNormalRecurrentReachedRootASN'] = lastChangeASN
            toJSON['convergence']['lastNormalRecurrentReachedRootValue'] = lastValue
            print '\r\n'

            ''' This metric shows the number of ALL (also changed) tracks or unique IDS that actually are in the root's received reservation list at the last change of this reservation list. '''
            lastChangeASN = None
            lastValue = None
            for asn, val in self.engine.reachedRootConvergence.iteritems():
                if val != lastValue:
                    lastValue = val
                    lastChangeASN = asn
            cycle = -1
            if lastChangeASN != None:
                cycle = int(lastChangeASN / self.settings.slotframeLength)
            print 'Last ALL TYPES Recurrent Reached root at ASN %s (cycle %s) to value: %s.' % (str(lastChangeASN), str(cycle), str(lastValue))
            toJSON['convergence']['lastRecurrentReachedRootASN'] = lastChangeASN
            toJSON['convergence']['lastRecurrentReachedRootValue'] = lastValue
            print '\r\n'

            ''' List of all unique IDs that are in the root at the end of the experiment.'''
            reachedRootList, nrReachedRoot = self.getRootReached()
            toJSON['convergence']['recurrentReachedRootList'] = reachedRootList
            print 'List of recurrent reservations (%d) that are in the root AT END EXPERIMENT: %s' % (nrReachedRoot, str(reachedRootList))
            print '\r\n'

            nrTotalChangedReachedRoot = self.getTotalChangedRootReached()
            toJSON['convergence']['recurrentTotalReachedRootChangedValue'] = nrTotalChangedReachedRoot
            print 'Total number of recurrent UPDATED reservations that reached the root THROUGHOUT the experiment: %s' % (str(nrTotalChangedReachedRoot))

            ''' Get the total number of tracks that reached the root, during the whole experiment. '''
            nrTotalReachedRoot = self.getTotalRootReached()
            toJSON['convergence']['recurrentTotalReachedRootValue'] = nrTotalReachedRoot
            print 'Total number of ALL recurrent reservations that reached the root THROUGHOUT the experiment: %s' % (str(nrTotalReachedRoot))

            ''' List of all unique IDs that reached the root throughout the experiment.'''
            reachedRootAllList = self.getTotalRootReachedList()
            toJSON['convergence']['recurrentReachedRootAllList'] = reachedRootAllList
            print 'List of recurrent reservations (%d) that reached the root THROUGHOUT the experiment: %s' % (len(reachedRootAllList), str(reachedRootAllList))
            print '\r\n'

            # self.printOutRootReached()
            # reachedRoot, nrReachedRoot = self.getRootReached()
            # toJSON['convergence']['recurrentReservationsReachedRoot'] = nrReachedRoot
            # print '\r\n'

        toJSON['collisions'] = OrderedDict()
        if self.settings.sf == 'recurrent_chang':
            print 'Planned Possible Collisions (for Recurrent Scheduling) at TX side: %d' % self.engine.scheduledPossibleTXRecurrentCollisions
            toJSON['collisions']['scheduledPossibleTXRecurrentCollisions'] = self.engine.scheduledPossibleTXRecurrentCollisions
            print 'Planned Possible Collisions (for Recurrent Scheduling) at RX side: %d' % self.engine.scheduledPossibleRXRecurrentCollisions
            toJSON['collisions']['scheduledPossibleRXRecurrentCollisions'] = self.engine.scheduledPossibleRXRecurrentCollisions
        # print 'Effective TX collisions: %d' % self.engine.totalTXCollisions
        # toJSON['collisions']['totalTXCollisions'] = self.engine.totalTXCollisions
        # print 'Cycles of Effective TX collisions: %s' % str(self.engine.TXCollisionsCycles)
        # toJSON['collisions']['TXCollisionsCycles'] = self.engine.TXCollisionsCycles
        print '\r\n'

        toJSON['parentChanges'] = OrderedDict()
        totalListOfChanges = []
        minChanges = 100000
        minChangesID = None
        maxChanges = 0
        maxChangesID = None
        totalChanges = 0
        for mote, listOfChanges in self.engine.churnPrefParentDict.iteritems():
            if len(self.engine.churnPrefParentDict[mote]) < minChanges:
                minChanges = len(self.engine.churnPrefParentDict[mote])
                minChangesID = mote
            if len(self.engine.churnPrefParentDict[mote]) > maxChanges:
                maxChanges = len(self.engine.churnPrefParentDict[mote])
                maxChangesID = mote
            totalChanges += len(self.engine.churnPrefParentDict[mote])
        avgChanges = totalChanges / float(self.engine.settings.numMotes)
        print 'Changed Preferred Parent: %d times.' % self.engine.churnPrefParent
        toJSON['parentChanges']['churnPrefParent'] = self.engine.churnPrefParent
        print 'Total Preferred Parent Changes (should match previous line): %d' % totalChanges
        toJSON['parentChanges']['totalChanges'] = totalChanges
        print 'Average Preferred Parent Changes per Mote: %.4f' % avgChanges
        toJSON['parentChanges']['avgChanges'] = avgChanges
        print 'Min Preferred Parent Changes: %d (mote %s)' % (minChanges, str(minChangesID))
        toJSON['parentChanges']['minChanges'] = minChanges
        print 'Max Preferred Parent Changes: %d (mote %s)' % (maxChanges, str(maxChangesID))
        toJSON['parentChanges']['maxChanges'] = maxChanges
        print 'Changes per mote dictionary:'
        toJSON['parentChanges']['listOfChanges'] = OrderedDict()
        for mote, listOfChanges in self.engine.churnPrefParentDict.iteritems():
            toJSON['parentChanges']['listOfChanges'][mote] = listOfChanges
            output = '%d: ' % mote
            for tupleOfChange in listOfChanges:
                output += ' -> %s' % str(tupleOfChange)
            print output
        print '\r\n'

        toJSON['battery'] = OrderedDict()
        toJSON['battery']['batteryPerMote'] = OrderedDict()
        for mote in self.engine.motes:
            if mote.id in self.batteryPerMote:
                toJSON['battery']['batteryPerMote'][mote.id] = self.batteryPerMote[mote.id][-1]
                # print 'Charge consumed by mote %d: %d' % (mote.id, self.batteryPerMote[mote.id][-1])
            else:
                pass
                # print 'Charge consumed by mote %d: %d' % (mote.id, 0)
        print '\r\n'

        toJSON['cells'] = OrderedDict()
        toJSON['cells']['txCellsPerMote'] = OrderedDict()
        for mote in self.engine.motes:
            if mote.id in self.engine.activeTXCells:
                toJSON['cells']['txCellsPerMote'][mote.id] = len(self.engine.activeTXCells[mote.id])
                # print 'Active TX cells on mote %d: %d' % (mote.id,  len(self.engine.activeTXCells[mote.id]))
            else:
                pass
                # print 'Active TX cells on mote %d: %d' % (mote.id,  0)
        toJSON['cells']['rxCellsPerMote'] = OrderedDict()
        for mote in self.engine.motes:
            if mote.id in self.engine.activeRXCells:
                toJSON['cells']['rxCellsPerMote'][mote.id] = len(self.engine.activeRXCells[mote.id])
                # print 'Active RX cells on mote %d: %d' % (mote.id,  len(self.engine.activeRXCells[mote.id]))
            else:
                pass
                # print 'Active RX cells on mote %d: %d' % (mote.id,  0)
        toJSON['cells']['sharedCellsPerMote'] = OrderedDict()
        for mote in self.engine.motes:
            if mote.id in self.engine.activeSharedCells:
                toJSON['cells']['sharedCellsPerMote'][mote.id] = len(self.engine.activeSharedCells[mote.id])
                # print 'Active shared cells on mote %d: %d' % (mote.id,  len(self.engine.activeSharedCells[mote.id]))
            else:
                pass
                # print 'Active shared cells on mote %d: %d' % (mote.id,  0)
        print '\r\n'

        # toJSON['topology'] = self.writeOutTopology()

        hopCount = []
        childrenCountMap = {}
        for mote in self.engine.motes:
            if mote.id != 0:
                hopCount.append(self.getHopCount(mote, 0))
            if mote.id != 0:
                if mote.preferredParent.id not in childrenCountMap:
                    childrenCountMap[mote.preferredParent.id] = 1
                else:
                    childrenCountMap[mote.preferredParent.id] += 1

        toJSON['topology'] = {}
        toJSON['topology']['avgHops'] = float(sum(hopCount)) / float(len(hopCount))
        print childrenCountMap
        toJSON['topology']['avgChildren'] = float(sum(childrenCountMap.values()))/len(childrenCountMap)
        print 'Topology | average # hops = %.4f' % toJSON['topology']['avgHops']
        print 'Topology | average # children = %.4f' % toJSON['topology']['avgChildren']
        # print 'Topology: %s' % str(toJSON['topology'])

        print 'Initial transmitting times of all motes:'
        sendingTimes = self.engine.getSendingTimes()
        print sendingTimes
        toJSON['data'] = {}
        toJSON['data']['starttimes'] = sendingTimes
        maxSendingTime = None
        maxSendingTimeMote = None
        minSendingTime = None
        minSendingTimeMote = None
        for mote in sendingTimes:
            if minSendingTime == None:
                minSendingTime = sendingTimes[mote][0]
                minSendingTimeMote = mote
            if maxSendingTime == None:
                maxSendingTime = sendingTimes[mote][0]
                maxSendingTimeMote = mote
            if sendingTimes[mote][0] < minSendingTime:
                minSendingTime = sendingTimes[mote][0]
                minSendingTimeMote = mote
            if sendingTimes[mote][0] > maxSendingTime:
                maxSendingTime = sendingTimes[mote][0]
                maxSendingTimeMote = mote

        toJSON['data']['earliestTime'] = minSendingTime
        toJSON['data']['earliestTimeMote'] = minSendingTimeMote
        toJSON['data']['latestTime'] = maxSendingTime
        toJSON['data']['latestTimeMote'] = maxSendingTimeMote
        print 'Earliest transmitting time: %d' % minSendingTime
        print 'Earliest transmitting mote: %d' % minSendingTimeMote
        print 'Latest transmitting time: %d' % maxSendingTime
        print 'Latest transmitting mote: %d' % maxSendingTimeMote

        pkPeriodsPerMote = self.engine.getPkPeriods()
        toJSON['data']['pkPeriods'] = {}
        totalTraffic = {}
        for mote in pkPeriodsPerMote:
            toJSON['data']['pkPeriods'][mote] = pkPeriodsPerMote[mote]
            # print 'Pk period for mote %d: %.4f' % (mote, pkPeriodsPerMote[mote])
            end = float(self.settings.numCyclesPerRun*self.settings.slotframeLength*self.settings.slotDuration)
            diff = float(self.settings.numCyclesPerRun*self.settings.slotframeLength*self.settings.slotDuration) - float(self.engine.firstDelays[mote])
            totalTraffic[mote] = diff / float(pkPeriodsPerMote[mote])
            print 'Mote %d: TX start = %.4f | TX stop = %.4f | Diff = %.4f |  Traffic = %.4f |  Total Est. # packets = %.4f' % (mote, float(self.engine.firstDelays[mote]), end, float(diff), float(pkPeriodsPerMote[mote]), totalTraffic[mote])

        print 'Pk period average: %.4f' % (np.mean(pkPeriodsPerMote.values()))
        print 'Total Est. # generated packets: %.4f' % (np.sum(totalTraffic.values()))

        # for mote, lst in self.engine.pkPeriodChanges.iteritems():
        #     print 'For mote %d: %s' % (mote, str(lst))
        #     print '\r\n'

        print '\r\n'
        print '----------------------- END REPORT -----------------------'
        print '\r\n'

        # self.createDelayBinsPlot(mote, self.delayPerSource[mote], strdate)
        dirplot = '%s/summary' % self.settings.simDataDir
        if not os.path.exists(dirplot):
            os.makedirs(dirplot)
        jsonFName = '%s/report_%s.json' % (dirplot, str(self.engine.randomSeed))
        with open(jsonFName, 'w') as outfile:
            json.dump(toJSON, outfile)

        dirplot = '%s/summary' % self.settings.simDataDir
        if not os.path.exists(dirplot):
            os.makedirs(dirplot)

        # logging battery = DISABLED
        # jsonFName = '%s/battery_%s.json' % (dirplot, str(self.engine.randomSeed))
        # with open(jsonFName, 'w') as outfile:
        #     json.dump(self.batteryPerMote, outfile)

        # logging cells = DISABLED
        # cellsJSON = OrderedDict()
        # cellsJSON['activeTXCells'] = self.engine.activeTXCells
        # cellsJSON['activeRXCells'] = self.engine.activeRXCells
        # cellsJSON['activeSharedCells'] = self.engine.activeSharedCells
        # jsonFName = '%s/cells_%s.json' % (dirplot, str(self.engine.randomSeed))
        # with open(jsonFName, 'w') as outfile:
        #     json.dump(cellsJSON, outfile)

        # if expDir == None:
        # self.createDelayBinsPlot(jsonFName)

    def writeOutTopology(self):
        # toJSON = OrderedDict()
        topology = OrderedDict()
        for i in range(len(self.engine.motes)):
            # neighborMap = # map neighobors to their RSSI
            (locationX, locationY) = self.engine.motes[i].getLocation()
            startASN = None
            period = None
            if self.engine.reservationTuples != None and i in self.engine.reservationTuples and self.engine.reservationTuples[i] != None:
                startASN = self.engine.reservationTuples[i][2]
                period = self.engine.reservationTuples[i][1]
            neighbors = OrderedDict()
            for j in range(len(self.engine.motes)):
                if self.engine.motes[i].id != self.engine.motes[j].id and self.engine.motes[j] in self.engine.motes[i].PDR:
                    neighbors[self.engine.motes[j].id] = self.engine.motes[i].getPDR(self.engine.motes[j])
            if self.engine.motes[i].id != 0:
                topology[self.engine.motes[i].id] = {'x': locationX, 'y': locationY, 'prefParent': str(self.engine.motes[i].preferredParent.id), 'neighbors': neighbors, "start": startASN, "period": period}
            else:
                topology[self.engine.motes[i].id] = {'x': locationX, 'y': locationY, 'prefParent': 'root', 'neighbors': neighbors, "start": startASN, "period": period}
        topology_file = '%s/topology_seed_%s.json' % (self.settings.simDataDir, self.engine.randomSeed)
        # with open(topology_file, 'w') as outfile:
        #     json.dump(topology, outfile)
        # print json.dumps(topology)
        return topology
