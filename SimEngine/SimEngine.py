#!/usr/bin/python
'''
\brief Discrete-event simulation engine.

\author Thomas Watteyne <watteyne@eecs.berkeley.edu>
\author Kazushi Muraoka <k-muraoka@eecs.berkeley.edu>
\author Nicola Accettura <nicola.accettura@eecs.berkeley.edu>
\author Xavier Vilajosana <xvilajosana@eecs.berkeley.edu>
'''

#============================ logging =========================================

import random
# random.seed(5)

import logging
class NullHandler(logging.Handler):
    def emit(self, record):
        pass
log = logging.getLogger('SimEngine')
log.setLevel(logging.ERROR)
log.addHandler(NullHandler())

#============================ imports =========================================

import threading

import Propagation
import Topology
import Mote
import TrafficManager
import TrafficManagerBart
import SimSettings
import json
import numpy as np
import datetime
# import matplotlib.pyplot as plt
import os
import traceback
import copy

from collections import OrderedDict


#============================ defines =========================================

#============================ body ============================================

class SimEngine(threading.Thread):

    #===== start singleton
    _instance      = None
    _init          = False

    INITIALIZATION_PHASE_LAST_ASN      = 2000
    INITIALIZATION_PHASE_OTF_THRESHOLD = 4
    RECURRENT_DELAY_BUFFER             = 1 # number of timeslots after packet generation we want our recurrent reservation timeslot (NOTE: this should be at least 1)

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super(SimEngine,cls).__new__(cls, *args, **kwargs)
        return cls._instance
    #===== end singleton

    def __init__(self,runNum=None,failIfNotInit=False):

        if failIfNotInit and not self._init:
            raise EnvironmentError('SimEngine singleton not initialized.')

        #===== start singleton
        if self._init:
            return
        self._init = True
        #===== end singleton

        self.randomGen = random.Random()

        # store params
        self.runNum                         = runNum

        # local variables
        self.dataLock                       = threading.RLock()
        self.pauseSem                       = threading.Semaphore(0)
        self.simPaused                      = False
        self.goOn                           = True
        self.asn                            = 0
        self.startCb                        = []
        self.endCb                          = []
        self.events                         = []
        self.settings                       = SimSettings.SimSettings()

        # vars to enable multiple slotframe lengths
        self.nextCycleASN                   = self.settings.sharedframeLength # should be this, otherwise the motes will not boot okay
        self.nextSwitchASN                  = self.settings.sharedframeLength # should be this, otherwise the motes will not boot okay
        self.cycle                          = 0
        self.mgmtFrameDone                  = True # should be True, otherwise the motes will not boot okay
        self.frameIndex                     = -1 # -1 is the mgmt frame, all numbers up represent the other frames
        self.lastMgmtLastASN                = self.settings.sharedframeLength - 1 # the ASN of the last happened mgmt frame

        self.packetsToSend                  = OrderedDict() # cycle to (sourceMote, ASN)

        self.holdOTFThreshold               = self.settings.otfThreshold

        self.successLLSFMotes               = []

        self.uniqueIDs                      = []

        self.trafficManager                 = None
        self.reservationTuples              = OrderedDict()
        self.changeReservationTuples        = OrderedDict()
        # these are the motes that change their packet generation frequency
        self.nrChangeDataMotes              = 1
        self.changeDataMotes                = []

        # mapping from ASN to number of motes that have at least one cell to their pref parent
        self.parentConvergence              = OrderedDict()
        self.firstReservationPacket         = None
        self.childConvergence               = OrderedDict()
        self.firstRecurrencePacket          = None
        self.recurrentConvergence           = OrderedDict()
        self.reachedRootConvergence         = OrderedDict()
        self.reachedNormalRootConvergence   = OrderedDict()
        self.churnPrefParent                = 0
        self.churnPrefParentDict            = OrderedDict()
        self.totalTXCollisions              = 0
        self.TXCollisionsCycles             = []
        self.scheduledPossibleTXRecurrentCollisions   = 0
        self.scheduledPossibleRXRecurrentCollisions   = 0
        self.activeTXCells                  = OrderedDict()
        self.activeRXCells                  = OrderedDict()
        self.activeSharedCells              = OrderedDict()

        self.delayPerSource                 = OrderedDict()

        self.sendDataMotes                  = None

        self.sendingTimes                   = OrderedDict()
        self.firstDelays                    = OrderedDict()
        self.pkPeriods                      = OrderedDict() # the packet period of each mote
        self.reservationDelays              = OrderedDict() # the delays for doing recurrent reservations
        self.isChangingFrequency            = []
        self.changeFrequencyInterval        = 1 # every 20 seconds a mote can change its data generation interval
        self.minimumDelay                   = None
        self.pkPeriodChanges                = OrderedDict()

        self.collisionPercentages           = []

        self.randomSeed = None
        # if the simulation has been giving fixed random seeds
        if self.settings.randomSeeds != '[]':
            # convert the string of random seeds to a real list of randoms
            listRandomSeeds = self.settings.randomSeeds.split('[')[1].split(']')[0].split(',')
            # if we do some simulations in parallel
            if self.settings.cpuID is not None:
                # for example, 20 runs total with 2 cpus is 10 runs/cpu
                self.randomSeed = float(listRandomSeeds[self.settings.cpuID + (self.runNum*self.settings.numCPUs)])
                print 'runNum: %d, cpu ID: %d, selected one: %.8f, list: %s.' % (self.runNum, self.settings.cpuID, self.randomSeed, str(listRandomSeeds))
            else: # if we have only one cpu but have multiple runs
                self.randomSeed = float(listRandomSeeds[self.runNum])
                print 'runNum: %d, selected one: %.8f, list: %s.' % (self.runNum, self.randomSeed, str(listRandomSeeds))
        else:
            # fixed random seed
            self.randomSeed = 0.0662
            print 'Random seed: %.4f' % self.randomSeed

        if self.randomSeed > 0.0:
            # seed it here once
            self.randomGen.seed(self.randomSeed)

        if self.randomSeed:
            if not self.settings.perfectLinks:
                self.trafficManager             = TrafficManager.TrafficManager(self.settings.pkPeriod, self.settings.pkPeriodStd, randomSeed=self.randomSeed)
            else:
                self.trafficManager             = TrafficManagerBart.TrafficManagerBart(25, self.settings.pkPeriod, randomSeed=self.randomSeed)
        else:
            if not self.settings.perfectLinks:
                self.trafficManager             = TrafficManager.TrafficManager(self.settings.pkPeriod, self.settings.pkPeriodStd)
            else:
                self.trafficManager             = TrafficManagerBart.TrafficManagerBart(25, self.settings.pkPeriod)

        # these are the motes that send recurrent data
        self.sendDataMotes = range(1, self.settings.numMotes)

        print '\r\n'
        print 'The motes that will send data: %s.' % str(self.sendDataMotes)
        # calculate the minimum ASN at which the motes can start sending data packets
        # keep in mind:  self.bootdelay = (self.settings.numMotes - self.id) * self.settings.bootFactor
        # the maximum boot delay of a mote is (self.settings.numMotes - 0) * self.settings.bootFactor
        # TODO: so after two additional bootdelays, we let them send. This should change to after LLSF convergence
        minimumDelay                = (self.settings.numMotes * self.settings.bootDelay) + 5 * self.settings.bootDelay
        self.minimumDelay           = minimumDelay
        print "The general delay: %.4f." % minimumDelay

        self.motes                          = None
        minimumDelayLambda                  = lambda idx: minimumDelay if idx in self.sendDataMotes else None # lambda to test that mote should send data

        for mote in self.sendDataMotes:
            # calculate the period
            pkPeriod = self.settings.pkPeriod # set fixed
            if self.settings.sampledTraffic: # if the packet generation frequency should be sampled, sample it
                pkPeriod = self.trafficManager.getThroughput()
            self.pkPeriods[mote] = pkPeriod
            print 'The packet period of mote %d is %.4f seconds.' % (mote, pkPeriod)

            randomStart        = 200 * self.randomGen.uniform(0.0, 1.0)
            generalDelay       = minimumDelay + self.settings.delayAfterBooting
            individualDelay    = randomStart
            totalDelay         = generalDelay + individualDelay
            asnFromDelay       = int(totalDelay / float(self.settings.slotDuration)) # the general minimal delay each node has to wait + the individual delay the mote waits
            self.firstDelays[mote] = totalDelay
            self.setSendingTime(mote, asnFromDelay)

            if totalDelay <= 0:
                raise BaseException('The delay is smaller than or equal to zero. delay = %.4f, delayBeforeSending = %.4f, slotDuration = %.4f, pkPeriod = %.4f, randomStart = %.4f.' % (totalDelay, self.settings.delayAfterBooting, self.settings.slotDuration, self.pkPeriod, randomStart))

            if self.settings.sf == 'recurrent' or self.settings.sf == 'recurrent_chang':
                self.reservationDelays[mote] = minimumDelay + randomStart # because the recurrent SF has special reservations, these should also be planned
                timeslot = 0
                interval = int((float(pkPeriod)/float(self.settings.slotDuration))) # calculate the interval
                startASN = asnFromDelay + self.RECURRENT_DELAY_BUFFER
                endASN = self.settings.numCyclesPerRun * self.settings.slotframeLength # keep it going until the end of the experiment
                reservationTuple = (timeslot, interval, startASN, endASN)
                self.reservationTuples[mote] = reservationTuple
                print 'The reservation tuple of mote %d is %s seconds.' % (mote, str(reservationTuple))

            # if self.settings.dynamicNetwork and mote in self.isChangingFrequency:
            if self.settings.dynamicNetwork:

                # first determine moments at which the mote could POSSIBLE change frequency
                start = asnFromDelay * self.settings.slotDuration
                stop = self.settings.numCyclesPerRun * self.settings.slotframeLength * self.settings.slotDuration
                diff = stop - start

                bufferOfChange = (diff * 0.1)

                lstOfPossibleMomentASNs = []
                moment = start + self.changeFrequencyInterval
                while start <= moment and moment <= stop:
                    momentASN = int(moment / self.settings.slotDuration)
                    if (start + bufferOfChange) <= moment and moment <= (stop - bufferOfChange):
                        lstOfPossibleMomentASNs.append(momentASN)
                    moment += self.changeFrequencyInterval

                lstOfChangeReservationTuples = []
                for mmtASN in lstOfPossibleMomentASNs:
                    prob = self.randomGen.uniform(0.0, 1.0)
                    if prob <= self.settings.changeFreqProbability:
                        changePkPeriod = pkPeriod
                        if self.settings.sampledTraffic: # if the packet generation frequency should be sampled, sample it
                            changePkPeriod = self.trafficManager.getThroughput()
                        changeStartASN = mmtASN
                        changeEndASN = self.settings.numCyclesPerRun * self.settings.slotframeLength
                        changeTimeslot = 0
                        changeInterval = int((float(changePkPeriod) / float(self.settings.slotDuration)))
                        changeReservationTuple = (changeTimeslot, changeInterval, changeStartASN, changeEndASN)
                        lstOfChangeReservationTuples.append(changeReservationTuple)

                # print 'For mote %d, list of moments on which it could change: %s' % (mote, str(lstOfPossibleMomentASNs))
                # print 'For mote %d, list of change tuples: %s' % (mote, str(lstOfChangeReservationTuples))

                if len(lstOfChangeReservationTuples) > 0:
                    self.changeReservationTuples[mote] = copy.deepcopy(lstOfChangeReservationTuples)
                    print '[app] On mote %d, the reservation the tuple will change to is %s.' % (mote, str(self.changeReservationTuples[mote]))

        print '\r\n'
        if self.randomSeed:
            # self.motes                      = [Mote.Mote(id, randomSeed=self.randomSeed+float(id), minimalASN=generalDelayLambda(id)) for id in range(self.settings.numMotes)]
            self.motes                      = [Mote.Mote(id, randomSeed=self.randomSeed+id) for id in range(self.settings.numMotes)]
        else:
            self.motes                      = [Mote.Mote(id) for id in range(self.settings.numMotes)]

        self.propagation                    = Propagation.Propagation(randomSeed=self.randomSeed)

        print '\r\n'
        # also give a seperate seed to the topology b/c there we use also numpy that has to be seeded
        self.topology                       = Topology.Topology(self.motes, randomSeed=self.randomSeed)
        if self.settings.topologyMode == 'random':
            print 'Creating a random topology.'
            self.topology.createTopology()
        elif self.settings.topologyMode == 'mesh':
            print 'Creating a mesh topology.'
            self.topology.createTopologyMesh()

        self.etx                            = [] # store the last etx values per mote

        self.successRun                     = False

        # set this before the motes boot, otherwise the frame lengths won't be set approriate when booting
        if self.settings.switchFrameLengths:
            self.settings.setFrameLength(self.settings.sharedframeLength)

        # boot all motes
        for i in range(len(self.motes)):
            self.motes[i].boot()

        # initialize parent class
        threading.Thread.__init__(self)
        self.name                           = 'SimEngine'

    def destroy(self):
        # destroy the propagation singleton
        self.propagation.destroy()

        # destroy my own instance
        self._instance                      = None
        self._init                          = False

    #======================== thread ==========================================

    # def getCurrentPkPeriod(self, mote):
    #     with self.dataLock:
    #         if mote not in self.changeReservationTuples:
    #              return self.getPkPeriod(mote)
    #         else:
    #             tpls = self.changeReservationTuples[mote]
    #             print 'Tuples: %s' % str(tpls)
    #             print self.asn
    #             period = self.getPkPeriod(mote) # start from the start period
    #             prevASN = -1
    #             for tpl in tpls: # it is an orderedDict and they are in the dict chronologically
    #                 if tpl[2] < prevASN:
    #                     raise BaseException('Tuples not orderd chronologically!')
    #                 prevASN = tpl[2]
    #                 if tpl[2] <= self.asn:
    #                     period = int(float(tpl[1]) * float(self.settings.slotDuration))
    #                     print 'period'
    #                     print period
    #                     return period # you should break after the first one you encounter
    #             return period

    def getPkPeriod(self, mote):
        with self.dataLock:
            if mote in self.pkPeriods:
                return self.pkPeriods[mote]
            else:
                return None

    def getFirstDelay(self, mote):
        with self.dataLock:
            if mote in self.firstDelays:
                return self.firstDelays[mote]
            else:
                return None

    def getReservationDelay(self, mote):
        with self.dataLock:
            if mote in self.reservationDelays:
                return self.reservationDelays[mote]
            else:
                return None

    def getPkPeriods(self):
        with self.dataLock:
            return self.pkPeriods

    def getFirstDelays(self):
        with self.dataLock:
            return self.firstDelays

    def getReservationDelays(self):
        with self.dataLock:
            return self.reservationDelays

    def getReservationTuple(self, mote):
        with self.dataLock:
            if mote in self.reservationTuples:
                return self.reservationTuples[mote]
            else:
                return None

    def getChangingReservationTuple(self, mote):
        with self.dataLock:
            if mote in self.changeReservationTuples:
                return self.changeReservationTuples[mote]
            else:
                return None

    def getChangingReservationTuples(self):
        with self.dataLock:
            return self.changeReservationTuples

    def setSendingTime(self, mote, asn):
        with self.dataLock:
            self.sendingTimes[mote] = (asn, (asn % self.settings.slotframeLength))

    def getSendingTimes(self):
        return self.sendingTimes

    def getEarliestSendingTime(self):
        return min(self.sendingTimes.values())

    def addUniqueID(self, idllsf):
        ''' Add an unique ID to the list of unique IDs. '''
        with self.dataLock:
            self.uniqueIDs.append(idllsf)

    def removeLLSFReservations(self):
        ''' Remove all LLSF reservations on all motes who have this reservation. '''
        with self.dataLock:
            for i in range(len(self.motes)):
                for uniqueID in self.uniqueIDs:
                    self.motes[i]._sixtop_remove_llsf_reservation(uniqueID)
            self.uniqueIDs = [] # clean all the unique IDs

    def removeLLSFReservation(self, uniqueID):
        ''' Remove the reservation with uniqueID on all motes who have this reservation. '''
        with self.dataLock:
            for i in range(len(self.motes)):
                self.motes[i]._sixtop_remove_llsf_reservation(uniqueID)
            self.uniqueIDs.remove(uniqueID)

    def changeGenerationFrequency(self):
        ''' Change the packet generation frequency on all the motes that we determined in the beginning of the experiment. '''
        with self.dataLock:
            for mote in self.motes:
                if mote.id in self.changeDataMotes:
                    # get the unique ID from the mote on which the packet generation frequency is changeDataMotes
                    # remove that reservation on all motes who have reservation
                    self.removeLLSFReservation(mote.getUniqueID())
                    mote.changeGenerationFrequency()

    def run(self):
        ''' event driven simulator, this thread manages the events '''

        try:
            # log
            log.info("thread {0} starting".format(self.name))

            if self.settings.switchFrameLengths:
                # schedule the endOfSimulation event
                # calculate the last ASN when switching frame lengths is enabled
                lastASN = 0
                count = 0
                nrNormalSlotFrame = -1
                while count < self.settings.numCyclesPerRun:
                    if nrNormalSlotFrame == -1:
                        lastASN += self.settings.sharedframeLength
                        nrNormalSlotFrame += 1
                    elif nrNormalSlotFrame > -1:
                        lastASN += self.settings.slotframeLength
                        nrNormalSlotFrame += 1

                    if nrNormalSlotFrame == self.settings.sharedframeInterval:
                        nrNormalSlotFrame = -1

                    count += 1

                self.scheduleAtAsn(
                    asn         = lastASN,
                    cb          = self._actionEndSim,
                    uniqueTag   = (None,'_actionEndSim'),
                )
                print 'THE last ASN should be %d' % lastASN
            else:
                # schedule the endOfSimulation event
                self.scheduleAtAsn(
                    asn         = self.settings.slotframeLength*self.settings.numCyclesPerRun,
                    cb          = self._actionEndSim,
                    uniqueTag   = (None,'_actionEndSim'),
                )

            # call the start callbacks
            for cb in self.startCb:
                cb()

            self.nextSwitchASN = 0 # what is the next ASN at which the slotframe length changes?
            self.mgmtFrameDone = False # was the last frame a 6P management frame?
            if self.settings.switchFrameLengths:
                # first frame should be a 6P frame, so at ASN sharedframeLength the next switch of slotframe lengths should happen
                self.nextSwitchASN = self.settings.sharedframeLength
                self.nextCycleASN = self.settings.sharedframeLength
                self.mgmtFrameDone = True

            self.holdOTFThreshold = self.settings.otfThreshold

            # consume events until self.goOn is False
            while self.goOn:

                with self.dataLock:

                    # abort simulation when no more events
                    if not self.events:
                        log.info("end of simulation at ASN={0}".format(self.asn))
                        break
                    #
                    # if self.asn == 0:
                    #     self._actionPauseSim()
                    #
                    # if self.asn == 420:
                    #     self._actionPauseSim()

                    # make sure we are in the future
                    (a,b,cb,c) = self.events[0]
                    if c[1] != '_actionPauseSim':
                           assert self.events[0][0] >= self.asn

                    # update the current ASN
                    self.asn = self.events[0][0]

                    # decrease the sixtop timers
                    for i in range(len(self.motes)):
                        self.motes[i]._sixtop_decrease_timers()
                        self.motes[i]._sixtop_decrease_timers_responses()
                        self.motes[i]._sixtop_check_delayed_reservations()

                    # WARNING: this has to be BEFORE next if statement
                    if self.settings.switchFrameLengths and self.asn == self.nextCycleASN: # update cycle
                        print 'finish cycle %d at %d' % (self.cycle, self.asn)

                        self.cycle += 1
                        if self.mgmtFrameDone: # if we just had a mgmt frame, now comes a default frame
                            self.nextCycleASN = self.asn + self.settings.slotframeLength
                            self.lastMgmtLastASN = self.nextCycleASN - 1
                        else:
                            if self.asn == self.nextSwitchASN: # if the next current asn is a switch asn, count a mgmt frame
                                self.nextCycleASN = self.asn + self.settings.sharedframeLength
                            else: # we will not switch (no switch asn), so keep the normal slotframe length
                                self.nextCycleASN = self.asn + self.settings.slotframeLength
                                self.frameIndex += 1

                    if self.settings.switchFrameLengths and self.asn == self.nextSwitchASN:
                        # first frame should be a 6P frame, so at ASN sharedframeLength the next switch of slotframe lengths should happen
                        if self.mgmtFrameDone:
                            self.nextSwitchASN = self.asn + self.settings.sharedframeInterval * self.settings.slotframeLength
                            self.mgmtFrameDone = False
                            self.settings.setFrameLength(self.settings.slotframeLength)
                            self.frameIndex = 0 # we start a new series of normal slotframes
                            print 'THIS WAS A MANAGEMENT FRAME and we switch at ASN %s' % (self.asn)
                        else:
                            self.nextSwitchASN = self.asn + self.settings.sharedframeLength
                            self.mgmtFrameDone = True
                            self.settings.setFrameLength(self.settings.sharedframeLength)
                            self.frameIndex = -1 # we start a mgmt frame
                            print 'THIS WAS A NORMAL SLOTFRAME and we switch at ASN %s' % (self.asn)

                        # we are entering a new slotframe, so every node should switch schedule!
                        for i in range(len(self.motes)):
                            self.motes[i].switchSchedule()

                    # if we are in the asn at the beginning of a cycle
                    if not self.settings.switchFrameLengths and self.asn % self.settings.slotframeLength == 0:
                        # look for LLSF reservations to enable!
                        for i in range(len(self.motes)):
                            # give the start asn and the end asn
                            self.motes[i]._sixtop_enable_llsf_reservations(self.asn, (self.asn + self.settings.slotframeLength-1))

                    # call callbacks at this ASN
                    while True:
                        if self.events[0][0]!=self.asn:
                            break
                        (_,_,cb,_) = self.events.pop(0)
                        cb()

                    # if we are in the asn before the next cycle
                    if not self.settings.switchFrameLengths and ((self.asn + 1) % self.settings.slotframeLength) == 0:
                        # print self.randomGen.random()
                        # look for LLSF reservations to enable!
                        for i in range(len(self.motes)):
                            self.motes[i]._sixtop_remove_llsf_reservations()
                            if self.motes[i].id != 0:
                                self.motes[i].appendPacketsInQueue()

            # self.writeOutTopology()
            # print self.parentConvergence
            # print self.childConvergence
            # print self.recurrentConvergence

            # self.writeReport()

            print 'COLLIISIONS:'
            print self.collisionPercentages

            # call the end callbacks
            for cb in self.endCb:
                cb()

            print self.successLLSFMotes
            print len(self.successLLSFMotes)

            # If this can be set true, the experiment probably ended successfully.
            self.successRun = True

            # rands = []
            # for index in range(100):
            #     rands.append(self.randomGen.random())
            # print rands

            # log
            log.info("thread {0} ends".format(self.name))
        except BaseException as e:
            errorfile = 'exceptionfile.plt'
            msg = '%s: Experiment run went wrong, with message: %s, e: %s, traceback: %s.' % (str(datetime.datetime.now()), e.message, e, traceback.format_exc())
            with open(errorfile, 'a') as outfile:
                outfile.write(msg)
                outfile.write('\r\n')
        except Exception as e:
            errorfile = 'exceptionfile.plt'
            msg = '%s: Experiment run went wrong, with message: %s, e: %s, traceback: %s.' % (str(datetime.datetime.now()), e.message, e, traceback.format_exc())
            with open(errorfile, 'a') as outfile:
                outfile.write(msg)
                outfile.write('\r\n')
        except:
            errorfile = 'exceptionfile.plt'
            msg = '%s: Some error: experiment run went wrong.' % (str(datetime.datetime.now()))
            with open(errorfile, 'a') as outfile:
                outfile.write(msg)
                outfile.write('\r\n')

    #======================== public ==========================================

    def addPacketsToSend(self, mID, cycle, asn):
        with self.dataLock:
            if cycle not in self.packetsToSend:
                self.packetsToSend[cycle] = [(mID, asn)]
            else:
                self.packetsToSend[cycle].append((mID, asn))
            # print self.packetsToSend

    def getPacketsToSend(self, mID, cycle):
        with self.dataLock:
            listOfSendASNs = []
            if cycle in self.packetsToSend:
                for (moteID, ASN) in self.packetsToSend[cycle]:
                    if moteID == mID:
                        listOfSendASNs.append(ASN)
            return listOfSendASNs

    def getSuccessRun(self):
        return self.successRun

    def getCycle(self):
        if self.settings.switchFrameLengths:
            return self.cycle
        else:
            # if we do not support frame length switching, return the normal cycle calculation
            return int(self.asn / self.settings.slotframeLength)

    # get the last ASN from this cycle
    def getEndASN(self):
        return self.nextCycleASN - 1

    # get the current timeslot in the current slotframe (length)
    def getTimeslot(self):
        timeslot = None
        if self.mgmtFrameDone: # if we're in a mgmt frames
            beginASN = self.nextCycleASN - self.settings.sharedframeLength
            timeslot = self.asn - beginASN
            # print 'Come here? at %d' % self.asn
        else: # if we're in a normal frame
            beginASN = self.nextCycleASN - self.settings.slotframeLength
            timeslot = self.asn - beginASN
            # print 'we come here?? at %d' % self.asn
        return timeslot

    def getNextFrameType(self):
        if self.frameIndex == -1 or self.frameIndex < (self.settings.sharedframeInterval - 1):
            return 'default'
        elif self.frameIndex == (self.settings.sharedframeInterval - 1):
            return 'mgmt'

    def getNextCycleEndASN(self):
        nextFrameLength = self.getNextFrameType
        if self.getNextFrameType() == 'default':
            nextFrameLength = self.settings.slotframeLength
        elif self.getNextFrameType() == 'mgmt':
            nextFrameLength = self.settings.sharedframeLength
        return (self.getEndASN() + nextFrameLength)


    def logActiveTXCell(self, moteID):
        with self.dataLock:
            if moteID not in self.activeTXCells:
                self.activeTXCells[moteID] = []
            self.activeTXCells[moteID].append(self.asn)

    def logActiveRXCell(self, moteID):
        with self.dataLock:
            if moteID not in self.activeRXCells:
                self.activeRXCells[moteID] = []
            self.activeRXCells[moteID].append(self.asn)

    def logActiveSharedCell(self, moteID):
        with self.dataLock:
            if moteID not in self.activeSharedCells:
                self.activeSharedCells[moteID] = []
            self.activeSharedCells[moteID].append(self.asn)

    #=== scheduling

    def scheduleAtStart(self,cb):
        with self.dataLock:
            self.startCb    += [cb]

    def scheduleIn(self,delay,cb,uniqueTag=None,priority=0,exceptCurrentASN=True):
        ''' used to generate events. Puts an event to the queue '''

        with self.dataLock:
            asn = int(self.asn+(float(delay)/float(self.settings.slotDuration)))

            self.scheduleAtAsn(asn,cb,uniqueTag,priority,exceptCurrentASN)

    def scheduleAtAsn(self,asn,cb,uniqueTag=None,priority=0,exceptCurrentASN=True):
        ''' schedule an event at specific ASN '''

        # make sure we are scheduling in the future
        assert asn>self.asn

        # remove all events with same uniqueTag (the event will be rescheduled)
        if uniqueTag:
            self.removeEvent(uniqueTag,exceptCurrentASN)

        with self.dataLock:

            # find correct index in schedule
            i = 0
            while i<len(self.events) and (self.events[i][0]<asn or (self.events[i][0]==asn and self.events[i][1]<=priority)):
                i +=1

            # add to schedule
            self.events.insert(i,(asn,priority,cb,uniqueTag))

    def removeEvent(self,uniqueTag,exceptCurrentASN=True):
        with self.dataLock:
            i = 0
            while i<len(self.events):
                if self.events[i][3]==uniqueTag and not (exceptCurrentASN and self.events[i][0]==self.asn):
                    self.events.pop(i)
                else:
                    i += 1

    def scheduleAtEnd(self,cb):
        with self.dataLock:
            self.endCb      += [cb]

    #=== play/pause

    def play(self):
        self._actionResumeSim()

    def pauseAtAsn(self,asn):
        if not self.simPaused:
            self.scheduleAtAsn(
                asn         = asn,
                cb          = self._actionPauseSim,
                uniqueTag   = ('SimEngine','_actionPauseSim'),
            )

    #=== getters/setters

    def getAsn(self):
        return self.asn

    # gdaneels
    def setETX(self, etx):
        with self.dataLock:
            self.etx.append(etx)

    def getETX(self):
        with self.dataLock:
            return self.etx
    #======================== private =========================================

    def _actionPauseSim(self):
        if not self.simPaused:
            self.simPaused = True
            self.pauseSem.acquire()

    def _actionResumeSim(self):
        if self.simPaused:
            self.simPaused = False
            self.pauseSem.release()

    def _actionEndSim(self):
        with self.dataLock:
            self.goOn = False
