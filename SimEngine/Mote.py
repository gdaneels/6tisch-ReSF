#!/usr/bin/python
'''
\brief Model of a 6TiSCH mote.

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
log = logging.getLogger('Mote')
log.setLevel(logging.DEBUG)
log.addHandler(NullHandler())

#============================ imports =========================================

import copy
import random
import threading
import math
import numpy as np

import CollisionSolver
import SimEngine
import SimSettings
import Propagation
import Topology
import TrafficManager
import traceback

from collections import OrderedDict

#============================ defines =========================================

#============================ body ============================================

class Mote(object):

    # sufficient num. of tx to estimate pdr by ACK
    NUM_SUFFICIENT_TX                  = 10
    # maximum number of tx for history
    NUM_MAX_HISTORY                    = 32

    DIR_TX                             = 'TX'
    DIR_RX                             = 'RX'
    DIR_TXRX_SHARED                    = 'SHARED'


    DEBUG                              = 'DEBUG'
    INFO                               = 'INFO'
    WARNING                            = 'WARNING'
    ERROR                              = 'ERROR'

    # DATA PACKET TAG
    TAG_LLSF                           = 'LLSF'

    #=== app
    APP_TYPE_DATA                      = 'DATA'
    RPL_TYPE_DIO                       = 'DIO'
    SIXTOP_TYPE_ADD                    = '6P_ADD'
    SIXTOP_TYPE_DELETE                 = '6P_DELETE'
    SIXTOP_TYPE_RESPONSE               = '6P_RESPONSE'
    SIXTOP_TYPE_DELETE_RESPONSE        = '6P_DELETE_RESPONSE'

    #=== rpl
    RPL_PARENT_SWITCH_THRESHOLD        = 768 # corresponds to 1.5 hops. 6tisch minimal draft use 384 for 2*ETX.
    RPL_MIN_HOP_RANK_INCREASE          = 256
    RPL_MAX_ETX                        = 4
    RPL_MAX_RANK_INCREASE              = RPL_MAX_ETX*RPL_MIN_HOP_RANK_INCREASE*2 # 4 transmissions allowed for rank increase for parents
    RPL_MAX_TOTAL_RANK                 = 256*RPL_MIN_HOP_RANK_INCREASE*2 # 256 transmissions allowed for total path cost for parents
    RPL_PARENT_SET_SIZE                = 1
    DEFAULT_DIO_INTERVAL_MIN           = 3 # log2(DIO_INTERVAL_MIN), with DIO_INTERVAL_MIN expressed in ms
    DEFAULT_DIO_INTERVAL_DOUBLINGS     = 20 # maximum number of doublings of DIO_INTERVAL_MIN (DIO_INTERVAL_MAX = 2^(DEFAULT_DIO_INTERVAL_MIN+DEFAULT_DIO_INTERVAL_DOUBLINGS) ms)
    DEFAULT_DIO_REDUNDANCY_CONSTANT    = 10 # number of hearings to suppress next transmission in the current interval

    #=== otf
    OTF_TRAFFIC_SMOOTHING              = 0.5
    #=== 6top
    # SIXTOP_TIMEOUT_INIT                = 404 # ASNs as an initial timeout value: this should be smaller than the housekeeping period
    #=== tsch
    TSCH_QUEUE_SIZE                    = 10
    TSCH_MAXTXRETRIES                  = 5
    # TSCH_SALOHA_MAXBACKOFF             = 2 # this should be smaller than the sixtoptimeout (when there is only 1 shared cell per slotframe)
    TSCH_SALOHA_MAXBACKOFF             = 9 # this should be smaller than the sixtoptimeout (when there is only 1 shared cell per slotframe)
    #=== radio
    RADIO_MAXDRIFT                     = 30 # in ppm
    #=== battery
    # see A Realistic Energy Consumption Model for TSCH Networks.
    # Xavier Vilajosana, Qin Wang, Fabien Chraim, Thomas Watteyne, Tengfei
    # Chang, Kris Pister. IEEE Sensors, Vol. 14, No. 2, February 2014.
    CHARGE_Idle_uC                     = 24.60
    CHARGE_TxDataRxAck_uC              = 64.82
    CHARGE_TxData_uC                   = 49.37
    CHARGE_RxDataTxAck_uC              = 76.90
    CHARGE_RxData_uC                   = 64.65

    # MAX_COLLISION_RATE                 = 0.05
    MAX_RESF_RESERVATIONS              = 6
    NUMBER_BACKUP_RESERVATIONS         = 4
    NUMBER_BACKUP_RESERVATIONS_NORMAL  = 5

    BROADCAST_ADDRESS                  = 0xffff

    RESERVATION_NOTFEASABLE             = 'NOTFEASABLE'

    # def __init__(self, id, randomSeed = None, minimalASN = None):
    def __init__(self, id, randomSeed = None):
        # random.seed(randomSeed)
        self.randomGen = random.Random()
        self.randomGen.seed(randomSeed)
        print 'On mote %d, randomSeed = %.4f.' % (id, randomSeed)
        # store params
        self.id                        = id
        # local variables
        self.dataLock                  = threading.RLock()

        self.engine                    = SimEngine.SimEngine()
        self.settings                  = SimSettings.SimSettings()
        self.propagation               = Propagation.Propagation()
        self.trafficManager            = TrafficManager.TrafficManager()

        # app
        self.pkPeriod                  = self.engine.getPkPeriod(id)
        self.firstDelay                = self.engine.getFirstDelay(id)
        # role
        self.dagRoot                   = False
        # rpl
        self.rank                      = None
        self.dagRank                   = None
        self.parentSet                 = []
        self.preferredParent           = None
        self.rplRxDIO                  = OrderedDict()                    # indexed by neighbor, contains int
        self.neighborRank              = OrderedDict()                    # indexed by neighbor
        self.neighborDagRank           = OrderedDict()                    # indexed by neighbor
        self.trafficPortionPerParent   = OrderedDict()                    # indexed by parent, portion of outgoing traffic
        # otf
        self.asnOTFevent               = None
        self.otfHousekeepingPeriod     = self.settings.otfHousekeepingPeriod
        self.timeBetweenOTFevents      = []
        self.inTraffic                 = OrderedDict()                    # indexed by neighbor, gdaneels, for keeping traffic to parent
        self.inTrafficLLSF             = OrderedDict()
        self.inTrafficTo               = OrderedDict()                    # gdaneels, indexed by neighbor, for keeping traffic to children
        self.inTrafficMovingAve        = OrderedDict()                    # indexed by neighbor
        self.inTrafficMovingAveTo      = OrderedDict()                    # gdaneels, indexed by neighbor, for keeping traffic to children
        self.asnLastOTFCall            = 0                     # the asn of the last time the housekeeping was executing, no matter the otf was triggered (different from self.asnOTFEvent)
        self.mgmtSlotsToSubstract      = 0                     # the number of shared cells that have to be substracted from the total time that a mote could send (see otf housekeeping)
        self.blockedLLSFReservationsRXTuples = []                    # gdaneels, hold (ts, ch, recurrencyPeriod, endTime) that are blocked and yet have to be confirmed
        self.LLSFReservationsRXTuples        = []                    # gdaneels, hold (ts, ch, transID, smac)
        self.blockedLLSFReservationsTXTuples = []                    # gdaneels, hold (ts, ch, recurrencyPeriod, endTime) that are blocked and yet have to be confirmed
        self.LLSFReservationsTXTuples        = []                    # gdaneels, hold (ts, ch, transID, smac)
        # 6top
        self.numCellsToNeighbors       = OrderedDict()                    # indexed by neighbor, contains int
        self.numCellsFromNeighbors     = OrderedDict()                    # indexed by neighbor, contains int
        self.numLLSFCellsToNeighbors   = OrderedDict()                    # indexed by neighbor, contains int
        self.numLLSFCellsFromNeighbors = OrderedDict()                    # indexed by neighbor, contains int
        self.sixtopTransactionTX       = OrderedDict()                    # for the transmitter side, containing the 6P transaction id, number of cells to be reserved, direction and int (timer value) that indicates the 6P transactions is ongoing, True/False if it is a LLSF transaction
        self.sixtopDeletionTX          = OrderedDict()                    # for the transmitter side, containing the 6P transaction id, the cells to be deleted and the int (timer value) indicating the deletion is ongoing
        self.sixtopTransactionTXResponses = OrderedDict()                    # for the transmitter side, containing the 6P transaction id, number of cells to be reserved, direction and int (timer value) that indicates the 6P transactions is ongoing, True/False if it is a LLSF transaction
        self.sixtopDeletionTXResponses = OrderedDict()

        # changing this threshold the detection of a bad cell can be
        # tuned, if as higher the slower to detect a wrong cell but the more prone
        # to avoid churn as lower the faster but with some chances to introduces
        # churn due to unstable medium
        self.sixtopPdrThreshold           = self.settings.sixtopPdrThreshold
        self.sixtopHousekeepingPeriod  = self.settings.sixtopHousekeepingPeriod
        # tsch
        self.txQueue                   = []
        self.pktToSend                 = None
        self.defaultSchedule           = OrderedDict()                    # this is the default schedule
        self.mgmtSchedule              = OrderedDict()                    # this is the mgmt schedule
        self.schedule                  = OrderedDict()                    # indexed by ts, contains cell: gdaneels, this will reference the default or mgmt schedule
        self.currentScheduleName       = 'mgmt'
        self.waitingFor                = None
        self.timeCorrectedSlot         = None
        self.blockedCellList             = []
        # radio
        self.txPower                   = 0                     # dBm
        self.antennaGain               = 0                     # dBi
        self.minRssi                   = self.settings.minRssi # dBm
        self.noisepower                = -105                  # dBm
        self.drift                     = self.randomGen.uniform(-self.RADIO_MAXDRIFT, self.RADIO_MAXDRIFT)
        print 'On mote %d: drift %.4f.' % (self.id, self.drift)
        # wireless
        self.RSSI                      = OrderedDict()                    # indexed by neighbor
        self.PDR                       = OrderedDict()                    # indexed by neighbor
        # location
        # battery
        self.chargeConsumed            = 0

        self.firstPacket = True
        self.lastPacketSentASN = None

        self.rSeed = randomSeed

        # the delay (second) for this mote to start doing rpl dio and otf housekeeping
        self.bootDelay                 = (self.settings.numMotes - self.id) * self.settings.bootDelay

        # send data or not (given in constructor)
        # self.minimalASN                  = minimalASN

        # it should be smaller than the otf housekeeping period, so take the houskeeping period and substract one slotframe length
        # self.sixtop_timout_init        = (self.settings.otfHousekeepingPeriod / float(self.settings.slotDuration)) - self.settings.slotframeLength
        # self.sixtop_timout_init        = (self.settings.otfHousekeepingPeriod * self.settings.slotframeLength) - self.settings.slotframeLength
        self.sixtop_timout_init          = 505
        # dictionary with relative numbers on traffic coming in from child neighbors
        # self.trafficPortionPerRxNeighbor = OrderedDict()

        # This should be the transID identifier tag of the packets generated a this mote.
        self.tagTransID = None
        self.tagUniqueID = None

        # map from llsf unique id that made llsf reservation to this mote to (transID_incoming_reservation, transID_outgoing_reservation)
        # these can actually be used to see if packets are received in the correct recurrent cell
        self.llsfTransIDs = OrderedDict()

        # transID, ASN, parent, reservation_tuple
        self.delayedLLSFRequests = []

        # map unique IDs to see if they are finished or not
        self.llsfRequests = OrderedDict()

        # If the packet generation changed, this should be set to True
        self.changedFrequency = False

        # The tuple in which the (first) LLSF reservation is saved. Used for the JSON output.
        self.reservationTuple = None

        # this is the period in which we want no collision between a normal and recurrent reservation
        # starting from the current ASN
        self.bufferPeriod = 10 * self.settings.slotframeLength

        self.nrPacket = 0

        self.collisionPercentages = []
        self.packetsInQueue = []

        # stats
        self._stats_resetMoteStats()
        self._stats_resetQueueStats()
        self._stats_resetLatencyStats()
        self._stats_resetHopsStats()
        self._stats_resetRadioStats()

    def switchSchedule(self):
        print 'Switching schedule on mote %d.' % self.id
        if self.currentScheduleName == 'default':
            self.schedule = self.mgmtSchedule
            self.currentScheduleName = 'mgmt'
        elif self.currentScheduleName == 'mgmt':
            self.schedule = self.defaultSchedule
            self.currentScheduleName = 'default'
        else:
            assert False

    def getUniqueID(self):
        return self.tagUniqueID

    #======================== stack ===========================================

    #===== role

    def role_setDagRoot(self):
        self.dagRoot              = True
        self.rank                 = 0
        self.dagRank              = 0
        self.packetLatencies      = [] # in slots
        self.packetHops           = []

    def resetPacketsInQueue(self):
        with self.dataLock:
            self.packetsInQueue = []

    def getAveragePacketsInQueue(self):
        with self.dataLock:
            return (sum(self.packetsInQueue) / len(self.packetsInQueue))

    def appendPacketsInQueue(self):
        with self.dataLock:
            # count = 0
            # for pkt in self.txQueue:
            #     if pkt['type'] == self.APP_TYPE_DATA and pkt['payload'][3] != None:
            #         reqCells += 1
            count = len(self.txQueue)
            # print 'On mote %d, there are %d packets in the queue.' % (self.id, count)
            self.packetsInQueue.append(count)
            # print 'On mote %d, list of packets in queue looks now like: %s.' % (self.id, self.packetsInQueue)

    def checkParentConvergence(self):
        with self.dataLock:
            countConverged = 0
            # check if it is converged right now.
            for i in range(len(self.engine.motes)):
                if self.engine.motes[i] != 0 and self.engine.motes[i].preferredParent is not None: # the mote should not have a preferred parent for this
                    if len(self.engine.motes[i].getTxCellsNeighborNoRecurrent(self.engine.motes[i].preferredParent)) > 0:
                        countConverged += 1
            # print 'On mote %d, countConverged = %d.' % (self.id, countConverged)
            if countConverged == (len(self.engine.motes) - 1):
                return True

            # check if it converged in one of the previous cycles (watchout, this metric is only logged per cycle)
            for converged in self.engine.parentConvergence.values():
                # print 'On mote %d, converged = %d.' % (self.id, converged)
                if converged == (len(self.engine.motes) - 1):
                    return True
            return False

    def resfConvergence(self):
        countConverged = 0
        # first check if at this time there are enough reservatoins
        for i in range(len(self.engine.motes)):
            if self.engine.motes[i].id in self.engine.sendDataMotes:
                tag = self.engine.motes[i].tagUniqueID
                if tag is not None and '_c' not in tag:
                    for t in self.engine.motes[0].LLSFReservationsRXTuples:
                        if tag == t[7]: # found the tag at the root
                            countConverged += 1
                            break
                else:
                    break
        # print countConverged
        if countConverged == len(self.engine.sendDataMotes):
            return True

        # check if there were enough in the past, that is also good enough
        # check if it converged in one of the previous cycles (watchout, this metric is only logged per cycle)
        for converged in self.engine.reachedNormalRootConvergence.values():
            # print 'On mote %d, ReSF converged = %d.' % (self.id, converged)
            if converged == (len(self.engine.motes) - 1):
                return True
        return False

    # def resfConvergenceNumber(self):
    #     countConverged = 0
    #     # first check if at this time there are enough reservatoins
    #     for i in range(len(self.engine.motes)):
    #         if self.engine.motes[i].id in self.engine.sendDataMotes:
    #             tag = self.engine.motes[i].tagUniqueID
    #             if tag is not None and '_c' not in tag:
    #                 for t in self.engine.motes[0].LLSFReservationsRXTuples:
    #                     if tag == t[7]: # found the tag at the root
    #                         countConverged += 1
    #                         break
    #             else:
    #                 break
    #     # print countConverged
    #     if countConverged == len(self.engine.sendDataMotes):
    #         return True
    #
    #     # check if there were enough in the past, that is also good enough
    #     # check if it converged in one of the previous cycles (watchout, this metric is only logged per cycle)
    #     for converged in self.engine.reachedNormalRootConvergence.values():
    #         # print 'On mote %d, ReSF converged = %d.' % (self.id, converged)
    #         if converged == (len(self.engine.motes) - 1):
    #             return True
    #     return converged

    #===== application
    #
    # def _app_schedule_schedule_sendSinglePacket(self):
    #     delay = self.minimalASN * self.settings.slotDuration
    #     print 'Mote %d will start scheduling a packet in %d seconds (at ASN %d).' % (self.id, delay, self.minimalASN)
    #     # schedule
    #     self.engine.scheduleIn(
    #         delay            = delay,
    #         cb               = self._app_schedule_sendSinglePacket,
    #         uniqueTag        = (self.id, '_app_schedule_sendSinglePacket'),
    #         priority         = 2,
    #     )


    def _app_schedule_recurrentReservation(self):
        # schedule
        self.engine.scheduleIn(
            delay            = self.engine.getReservationDelay(self.id),
            cb               = self._app_action_recurrentReservation,
            uniqueTag        = (self.id, '_app_action_recurrentReservation'),
            priority         = 2,
        )

    def _app_action_recurrentReservation(self):
        if (self.settings.sf == 'recurrent' or self.settings.sf == 'recurrent_chang') and self.preferredParent:
            reservationTuple = self.engine.getReservationTuple(self.id)
            self._log(
                self.INFO,
                "[app] On mote {0}, making a ReSF reservation at {1}",
                (self.id, str(reservationTuple)),
            )

            # make a new unique ID for the LLSF reservation, unique for this 'stream'
            self.tagUniqueID = '%s_%s' % (self.id, self.engine.asn)
            # add it to the engine so he knows about the LLSF reservation
            self.engine.addUniqueID(self.tagUniqueID)
            # for now assume you know when these arrive
            # set the tag that you give to packets generated a this mote
            self.tagTransID = self._sixtop_cell_llsf_reservation_request(self.preferredParent, reservationTuple, llsfUniqueID=self.tagUniqueID)

            if not self.tagTransID:
                # add again to delayedLLSFRequests
                # the timer value, add one to only do the reservation one after
                timerValue = self.sixtopTransactionTX[self.preferredParent][3] + 1
                newASN = self.engine.asn + timerValue # try again when this is done.
                self.delayedLLSFRequests.append((newASN, None, self.preferredParent, reservationTuple, self.tagUniqueID,1))
            elif self.tagTransID == self.RESERVATION_NOTFEASABLE:
                pass
            else:
                # add it to transID map
                if self.tagUniqueID not in self.llsfTransIDs:
                    self.llsfTransIDs[self.tagUniqueID] = []
                # add the new outgoing id
                self.llsfTransIDs[self.tagUniqueID].append((None, self.tagTransID))


    def _app_schedule_changingRecurrentReservation(self):
        if self.id in self.engine.getChangingReservationTuples():
            ''' Schedule all the changed reservation tuples. '''
            for tpl in self.engine.getChangingReservationTuple(self.id):
                newStartASN = tpl[2]
                # newStartASNinSeconds = newStartASN * float(self.settings.slotDuration)
                # schedule
                uniqueTag = '_app_action_changingRecurrentReservation_%s' % newStartASN
                self.engine.scheduleAtAsn(
                    asn              = newStartASN,
                    cb               = self._app_action_changingRecurrentReservation,
                    uniqueTag        = (self.id, uniqueTag),
                    priority         = 2,
                )
                self._log(
                    self.INFO,
                    "[app] On mote {0}, planning changing ReSF reservation at ASN {1}",
                    (self.id, newStartASN),
                )
                # print '[app] On mote %d planning changing ReSF reservation at ASN %d' % (self.id, newStartASN)

    def _app_action_changingRecurrentReservation(self):
        ''' change the packet generation frequency '''
        with self.dataLock:
            changeReservationTuples = self.engine.getChangingReservationTuple(self.id)
            changeReservationTuple = None
            for tpl in changeReservationTuples:
                if tpl[2] == self.engine.asn: # if it is the current ASN, we have the correct ASN
                    changeReservationTuple = tpl
            if changeReservationTuple is None:
                raise BaseException('On mote %d, can not find correct reservation tuple at this ASN %d.' % (self.id, self.engine.asn))
            self._log(
                self.INFO,
                "[app] On mote {0}, changing to reservation tuple {1}",
                (self.id, str(changeReservationTuple)),
            )
            # print '[app] At %d, on mote %d, changing to reservation tuple %s' % (self.engine.asn, self.id, str(changeReservationTuple))

            # Change the period! This happens for all scheduling functions.
            self.pkPeriod = changeReservationTuple[1] * float(self.settings.slotDuration) # interval * slot duration = interval in seconds

            # ISSUE Remove current scheduled sending of single packet = FIXED, every send single packet action now has an unique tag
            # self.engine.removeEvent(uniqueTag=(self.id, '_app_action_sendSinglePacket'))
            # Schedule a new sending of a single packet: now with the new packet period delay.
            if (self.lastPacketSentASN + changeReservationTuple[1]) <= self.engine.asn:
                # SEND IMMEDIATELY
                # print '------------------------------------ Mote %d' % self.id
                self._app_action_sendSinglePacket_CF()
            else:
                # print '+++++++++++++++++++++++++++++++++++++ Mote %d' % self.id
                # SEND AT TIME = (self.lastPacketSentASN + NEW_PERIOD) - current asn
                delay = ((self.lastPacketSentASN + changeReservationTuple[1]) - self.engine.asn) * float(self.settings.slotDuration)
                self._app_schedule_sendSinglePacket_CF(delay)

            if self.settings.sf == 'recurrent_chang' or self.settings.sf == 'recurrent':

                # remove the current reservation from all motes in GOD MODE
                # for mote in self.engine.motes:
                #     mote._sixtop_remove_llsf_reservation(self.tagUniqueID)
                #     if self.tagUniqueID in mote.llsfTransIDs:
                #         del mote.llsfTransIDs[self.tagUniqueID]

                # self._sixtop_remove_llsf_reservation(self.tagUniqueID)
                # if self.tagUniqueID in self.llsfTransIDs:
                #     del self.llsfTransIDs[self.tagUniqueID]

                # print self.tagUniqueID
                self._log(
                    self.INFO,
                    "[app] Changing packet generation frequency: on this mote, deleted unique ID = {0}.",
                    str(self.tagUniqueID),
                )

                if '_c_' in self.tagUniqueID:
                    tagUID = self.tagUniqueID.split('_c_')[0]
                else:
                    tagUID = self.tagUniqueID
                self.tagUniqueID = '%s_c_%s' % (tagUID, str(self.engine.asn)) # change the ID
                # print self.tagUniqueID

                # make a new reservation
                self.tagTransID = self._sixtop_cell_llsf_reservation_request(self.preferredParent, changeReservationTuple, llsfUniqueID=self.tagUniqueID)

                if not self.tagTransID:
                    # add again to delayedLLSFRequests
                    # the timer value, add one to only do the reservation one after
                    timerValue = self.sixtopTransactionTX[self.preferredParent][3] + 1
                    newASN = self.engine.asn + timerValue # try again when this is done.
                    self.delayedLLSFRequests.append((newASN, None, self.preferredParent, changeReservationTuple, self.tagUniqueID,1))
                elif self.tagTransID == self.RESERVATION_NOTFEASABLE:
                    pass
                else:
                    # add it to transID map
                    if self.tagUniqueID not in self.llsfTransIDs:
                        self.llsfTransIDs[self.tagUniqueID] = []
                    # add the new outgoing id
                    self.llsfTransIDs[self.tagUniqueID].append((None, self.tagTransID))

            # raise -1

    def _app_schedule_sendSinglePacket(self):
        '''
        create an event that is inserted into the simulator engine to send the data according to the traffic
        '''
        if not self.firstPacket:
            # compute random delay
            # self.pkPeriod    = self.engine.getCurrentPkPeriod(self.id)
            delay            = self.pkPeriod
            # delay            = self.pkPeriod
            # print delay
        else:
            # compute initial time within the range of [next asn, next asn+pkPeriod]
            # delay            = self.settings.slotDuration + self.pkPeriod*random.random()
            # the first packet only has to be send after the minimal ASN and between [next asn, pkt period]
            delay           = self.firstDelay
        assert delay>0

        if self.id not in self.engine.pkPeriodChanges:
            self.engine.pkPeriodChanges[self.id] = []
        self.engine.pkPeriodChanges[self.id].append((self.engine.asn, delay/float(self.settings.slotDuration)))

        # uniqueTag = '_app_action_sendSinglePacket_%s' % str(self.engine.asn)
        uniqueTag = '_app_action_sendSinglePacket'
        # schedule
        self.engine.scheduleIn(
            delay            = delay,
            cb               = self._app_action_sendSinglePacket,
            uniqueTag        = (self.id, uniqueTag),
            priority         = 2,
        )

    def _app_schedule_sendSinglePacket_CF(self, delay=None):
        '''
        create an event that is inserted into the simulator engine to send the data according to the traffic
        '''

        pktDelay = None

        if not self.firstPacket:
            if delay is None:
                pktDelay            = self.pkPeriod
            else:
                pktDelay            = delay
        else:
            pktDelay = self.firstDelay
        assert pktDelay>0

        if self.id not in self.engine.pkPeriodChanges:
            self.engine.pkPeriodChanges[self.id] = []
        self.engine.pkPeriodChanges[self.id].append((self.engine.asn, pktDelay/float(self.settings.slotDuration)))

        # uniqueTag = '_app_action_sendSinglePacket_CF_%s' % str(self.engine.asn)
        uniqueTag = '_app_action_sendSinglePacket_CF'
        # print 'In schedule:'
        # schedule
        self.engine.scheduleIn(
            delay            = pktDelay,
            cb               = self._app_action_sendSinglePacket_CF,
            uniqueTag        = (self.id, uniqueTag),
            priority         = 2,
        )

    def _app_schedule_sendPacketBurst(self):
        ''' create an event that is inserted into the simulator engine to send a data burst'''

        # schedule numPacketsBurst packets at burstTimestamp
        for i in xrange(self.settings.numPacketsBurst):
            self.engine.scheduleIn(
                delay        = self.settings.burstTimestamp,
                cb           = self._app_action_enqueueData,
                uniqueTag    = (self.id, '_app_action_enqueueData_burst1'),
                priority     = 2,
            )
            self.engine.scheduleIn(
                delay        = 3*self.settings.burstTimestamp,
                cb           = self._app_action_enqueueData,
                uniqueTag    = (self.id, '_app_action_enqueueData_burst2'),
                priority     = 2,
            )

    def _app_action_sendSinglePacket(self):
        ''' actual send data function. Evaluates queue length too '''

        if self.firstPacket:
            # print self.id
            # print self.engine.asn
            # if self.settings.collisionRateThreshold >= 0.10:
            if not self.checkParentConvergence():
                raise BaseException('At %d and seed %.4f, not all nodes have cells to their parents yet.' % (self.engine.asn, self.rSeed))
            if self.settings.sf == 'recurrent_chang' and not self.resfConvergence():
                print self.engine.motes[0].LLSFReservationsRXTuples
                print len(self.engine.motes[0].LLSFReservationsRXTuples)
                raise BaseException('At %d and seed %.4f, not all ReSF reservations arrived yet to the root.' % (self.engine.asn, self.rSeed))
            self.firstPacket = False

        # print 'On mote %d, at ASN %d, enqueued a packet.' % (self.id, self.engine.asn)
        self._log(
            self.INFO,
            "[app] On mote {0}, at ASN {1}, enqueued a packet.",
            (self.id, self.engine.asn),
        )
        # enqueue data
        self._app_action_enqueueData()

        self.lastPacketSentASN = self.engine.asn

        # schedule next _app_action_sendSinglePacket
        self._app_schedule_sendSinglePacket()

    def _app_action_sendSinglePacket_CF(self):
        ''' actual send data function. Evaluates queue length too '''

        if self.firstPacket:
            if not self.checkParentConvergence():
                raise BaseException('At %d and seed %.4f, not all nodes have cells to their parents yet.' % (self.engine.asn, self.rSeed))
            if self.settings.sf == 'recurrent_chang' and not self.resfConvergence():
                print self.engine.motes[0].LLSFReservationsRXTuples
                print len(self.engine.motes[0].LLSFReservationsRXTuples)
                raise BaseException('At %d and seed %.4f, not all ReSF reservations arrived yet to the root.' % (self.engine.asn, self.rSeed))
            self.firstPacket = False

        # print 'On mote %d, at ASN %d, enqueued a packet.' % (self.id, self.engine.asn)
        self._log(
            self.INFO,
            "[app] On mote {0}, at ASN {1}, enqueued a packet.",
            (self.id, self.engine.asn),
        )

        # print 'In action:'
        self.engine.removeEvent((self.id,'_app_action_sendSinglePacket_CF'),exceptCurrentASN=True)
        # enqueue data
        self._app_action_enqueueData()

        self.lastPacketSentASN = self.engine.asn

        # schedule next _app_action_sendSinglePacket
        self._app_schedule_sendSinglePacket_CF()

    def _app_action_enqueueData(self):
        ''' enqueue data packet into stack '''

        # only start sending data if I have some TX cells
        # NOTE: COMMENTED THIS FOR THE EXPERIMENTS
        # if self.getTxCells():

        self.nrPacket += 1

        # create new packet
        newPacket = {
            'asn':            self.engine.getAsn(),
            'type':           self.APP_TYPE_DATA,
            'payload':        [self.id,self.engine.getAsn(),1, self.tagTransID, self.tagUniqueID, self.nrPacket], # the payload is used for latency and number of hops calculation
            'retriesLeft':    self.TSCH_MAXTXRETRIES
        }

        # update mote stats
        self._stats_incrementMoteStats('appGenerated')

        # enqueue packet in TSCH queue
        isEnqueued = self._tsch_enqueue(newPacket)

        if isEnqueued:
            # increment traffic if it is not recurrent
            # OR when it is recurrent but we do not have any reservations yet
            if self.settings.sf != 'recurrent' and self.settings.sf != 'recurrent_chang':
                self._otf_incrementIncomingTraffic(self)
        else:
            # update mote stats
            self._stats_incrementMoteStats('droppedAppFailedEnqueue')


    def _rpl_action_enqueueDIO(self):
        ''' enqueue DIO packet into stack '''

        # only start sending data if I have Shared cells
        if self.getSharedCells():

            # create new packet
            newPacket = {
                'asn':            self.engine.getAsn(),
                'type':           self.RPL_TYPE_DIO,
                'payload':        [self.rank], # the payload is the rpl rank
                'retriesLeft':    1 # do not retransmit broadcast
            }

            # update mote stats
            self._stats_incrementMoteStats('appGenerated')

            # enqueue packet in TSCH queue
            isEnqueued = self._tsch_enqueue(newPacket)

            if isEnqueued:
                # increment traffic
                self._otf_incrementIncomingTraffic(self)
            else:
                # update mote stats
                self._stats_incrementMoteStats('droppedAppFailedEnqueue')

    #===== rpl

    def _schedule_set_ETX(self):
        with self.dataLock:
            asn    = self.engine.getAsn()
            # schedule at start of next cycle
            self.engine.scheduleIn(
                delay       = 1, # every second
                cb          = self._action_set_ETX,
                uniqueTag   = (self.id,'_action_set_ETX'),
                priority    = 3,
            )

    def _action_set_ETX(self,first=False):
        ''' Setting ETX for logging purposes '''
        with self.dataLock:
            if self.preferredParent != None:
                self.engine.setETX(self._estimateETX(self.preferredParent))
            self._schedule_set_ETX()

    # def _rpl_schedule_sendDIO(self,firstDIO=False):
    #
    #     with self.dataLock:
    #
    #         asn    = self.engine.getAsn()
    #
    #         futureAsn = None
    #         if not firstDIO:
    #             futureAsn = int(math.ceil(
    #                 random.uniform(0.8 * self.settings.dioPeriod, 1.2 * self.settings.dioPeriod) / (self.settings.slotDuration)))
    #         else:
    #             # futureAsn = int(self.engine.asn+(float(self.bootDelay)/float(self.settings.slotDuration)))
    #             futureAsn = int(self.engine.asn+(float(10*(0.9+0.1*random.random()))/float(self.settings.slotDuration)))
    #             print 'On mote %d, first ASN to send the DIO will be %d.' % (self.id, futureAsn)
    #             # futureAsn = 1
    #
    #         # if self.preferredParent != None:
    #         #     self.engine.setETX(self._estimateETX(self.preferredParent))
    #
    #         # schedule at start of next cycle
    #         self.engine.scheduleAtAsn(
    #             asn         = asn + futureAsn,
    #             cb          = self._rpl_action_sendDIO,
    #             uniqueTag   = (self.id,'_rpl_action_sendDIO'),
    #             priority    = 3,
    #         )

    # def _rpl_action_receiveDIO(self,type,smac,payload):
    #
    #     with self.dataLock:
    #
    #         if self.dagRoot:
    #             return
    #
    #         # update my mote stats
    #         self._stats_incrementMoteStats('rplRxDIO')
    #
    #         sender = smac
    #
    #         rank = payload[0]
    #
    #         # don't update poor link
    #         if self._rpl_calcRankIncrease(sender)>self.RPL_MAX_RANK_INCREASE:
    #             return
    #
    #         # update rank/DAGrank with sender
    #         self.neighborDagRank[sender]    = rank / self.RPL_MIN_HOP_RANK_INCREASE
    #         self.neighborRank[sender]       = rank
    #
    #         # update number of DIOs received from sender
    #         if sender not in self.rplRxDIO:
    #                 self.rplRxDIO[sender]   = 0
    #         self.rplRxDIO[sender]          += 1
    #
    #         # housekeeping
    #         self._rpl_housekeeping()
    #
    #         # update time correction
    #         if self.preferredParent == sender:
    #             asn                         = self.engine.getAsn()
    #             self.timeCorrectedSlot      = asn

    # def _rpl_action_sendDIO(self):
    #
    #     with self.dataLock:
    #
    #         if self.preferredParent or self.dagRoot:
    #             self._rpl_action_enqueueDIO()
    #             self._stats_incrementMoteStats('rplTxDIO')
    #
    #         self._rpl_schedule_sendDIO() # schedule next DIO


    def _rpl_schedule_sendDIO(self,firstDIO=False):

        with self.dataLock:

            asn    = self.engine.getAsn()
            ts     = asn%self.settings.slotframeLength

            if not firstDIO:
                cycle = int(math.ceil(self.settings.dioPeriod/(self.settings.slotframeLength*self.settings.slotDuration)))
            else:
                cycle = 1

            # schedule at start of next cycle
            self.engine.scheduleAtAsn(
                asn         = asn-ts+cycle*self.settings.slotframeLength,
                cb          = self._rpl_action_sendDIO,
                uniqueTag   = (self.id,'_rpl_action_sendDIO'),
                priority    = 3,
            )

    def _rpl_action_sendDIO(self):

        with self.dataLock:

            if self.rank!=None and self.dagRank!=None:

                # update mote stats
                self._stats_incrementMoteStats('rplTxDIO')

                # "send" DIO to all neighbors
                for neighbor in self._myNeigbors():

                    # don't update DAGroot
                    if neighbor.dagRoot:
                        continue

                    # don't update poor link
                    if neighbor._rpl_calcRankIncrease(self)>self._rpl_getMaxRankIncrease():
                        continue

                    # in neighbor, update my rank/DAGrank
                    neighbor.neighborDagRank[self]    = self.dagRank
                    neighbor.neighborRank[self]       = self.rank

                    # in neighbor, update number of DIOs received
                    if self not in neighbor.rplRxDIO:
                        neighbor.rplRxDIO[self]       = 0
                    neighbor.rplRxDIO[self]          += 1

                    # update my mote stats
                    self._stats_incrementMoteStats('rplRxDIO') # TODO: TX DIO?

                    # skip useless housekeeping
                    if not neighbor.rank or self.rank<neighbor.rank:
                        # in neighbor, do RPL housekeeping
                        #print 'ON MOTE %d, -----------------------------------------------------------------------------------------' % self.id
                        neighbor._rpl_housekeeping()

                    # update time correction
                    if neighbor.preferredParent == self:
                        asn                        = self.engine.getAsn()
                        neighbor.timeCorrectedSlot = asn

            # schedule to send the next DIO
            self._rpl_schedule_sendDIO()

    def _rpl_getMinimumSwitchThreshold(self):
        with self.dataLock:
            asnMinimumDelay = int(self.engine.minimumDelay / float(self.settings.slotDuration))
            if self.engine.asn < asnMinimumDelay:
                return self.RPL_PARENT_SWITCH_THRESHOLD
            else:
                return 10000

    def _rpl_getMaxRankIncrease(self):
        with self.dataLock:
            asnMinimumDelay = int(self.engine.minimumDelay / float(self.settings.slotDuration))
            if self.engine.asn < asnMinimumDelay:
                return self.RPL_MAX_RANK_INCREASE
            else:
                return 10000

    def _rpl_housekeeping(self):
        with self.dataLock:
            #===
            # refresh the following parameters:
            # - self.preferredParent
            # - self.rank
            # - self.dagRank
            # - self.parentSet

            oldParentSet = None

            # calculate my potential rank with each of the motes I have heard a DIO from
            potentialRanks = OrderedDict()
            allPotentialRanks = OrderedDict()
            allPotentialRankIncreases = OrderedDict()
            for (neighbor,neighborRank) in self.neighborRank.items():
                # calculate the rank increase to that neighbor
                # if neighbor.preferredParent != self:
                # print neighbor
                rankIncrease = self._rpl_calcRankIncrease(neighbor)
                if rankIncrease != None:
                    allPotentialRanks[neighbor] = neighborRank+rankIncrease
                    allPotentialRankIncreases[neighbor] = rankIncrease
                else:
                    allPotentialRanks[neighbor] = -1
                    allPotentialRankIncreases[neighbor] = -1
                if rankIncrease!=None and rankIncrease<=min([self._rpl_getMaxRankIncrease(), self.RPL_MAX_TOTAL_RANK-neighborRank]):
                    # record this potential rank
                    #ESTEBANS: check if there is a loop and if exists, skip the neighbor
                    rootReached=False
                    skipNeighbor=False
                    inode=neighbor
                    while rootReached==False:
                        if inode.preferredParent!=None:
                            if inode.preferredParent.id==self.id:
                                skipNeighbor=True
                            if inode.preferredParent.id==0:
                                rootReached=True
                            else:
                                inode=inode.preferredParent
                        else:
                            rootReached=True
                    if skipNeighbor==True:
                        continue # skip this neighbor because this can cause a routing loop

                    potentialRanks[neighbor] = neighborRank+rankIncrease

            # sort potential ranks
            sorted_potentialRanks = sorted(potentialRanks.iteritems(), key=lambda x:x[1])

            # switch parents only when rank difference is large enough
            for i in range(1,len(sorted_potentialRanks)):
                if sorted_potentialRanks[i][0] in self.parentSet:
                    # compare the selected current parent with motes who have lower potential ranks
                    # and who are not in the current parent set
                    for j in range(i):
                        if sorted_potentialRanks[j][0] not in self.parentSet:
                            if sorted_potentialRanks[i][1]-sorted_potentialRanks[j][1]<self._rpl_getMinimumSwitchThreshold():
                                mote_rank = sorted_potentialRanks.pop(i)
                                sorted_potentialRanks.insert(j,mote_rank)
                                break

            # pick my preferred parent and resulting rank
            if sorted_potentialRanks:
                oldParentSet = set([parent.id for parent in self.parentSet])

                (newPreferredParent,newrank) = sorted_potentialRanks[0]
                # asnMinimumDelay = int(self.engine.minimumDelay / float(self.settings.slotDuration))
                # if self.engine.asn >= asnMinimumDelay:
                #     for i in range(0,len(sorted_potentialRanks)):
                #         if sorted_potentialRanks[i][0] == self.preferredParent:
                #             (newPreferredParent,newrank) = sorted_potentialRanks[i]
                #             break

                # compare a current preferred parent with new one
                if self.preferredParent and newPreferredParent!=self.preferredParent:
                    for (mote,rank) in sorted_potentialRanks[:self.RPL_PARENT_SET_SIZE]:

                        if mote == self.preferredParent:
                            # switch preferred parent only when rank difference is large enough
                            if rank-newrank<self._rpl_getMinimumSwitchThreshold():
                                (newPreferredParent,newrank) = (mote,rank)

                    # update mote stats
                    self._stats_incrementMoteStats('rplChurnPrefParent')
                    self.engine.churnPrefParent += 1
                    if self.id not in self.engine.churnPrefParentDict:
                        self.engine.churnPrefParentDict[self.id] = []
                    self.engine.churnPrefParentDict[self.id].append((self.preferredParent.id, newPreferredParent.id, self.engine.asn))
                    # log
                    self._log(
                        self.INFO,
                        "[rpl] churn: preferredParent {0}->{1} (minimum switch {2})",
                        (self.preferredParent.id,newPreferredParent.id, self._rpl_getMinimumSwitchThreshold()),
                    )

                    # for (n, r) in potentialRanks.iteritems():
                    #     print '[POT] on mote %d, node %d and newrank %d' % (self.id, n.id, r)
                    #
                    # for (n, r) in allPotentialRanks.iteritems():
                    #     print '[ALLPOT] on mote %d, node %d and newrank %d' % (self.id, n.id, r)
                    #
                    # for (n, r) in allPotentialRankIncreases.iteritems():
                    #     print '[ALLPOTINCREASES] on mote %d, node %d and newrank %d' % (self.id, n.id, r)
                    #
                    # for (n, r) in sorted_potentialRanks:
                    #     print 'on mote %d, node %d and newrank %d' % (self.id, n.id, r)

                # update mote stats
                if self.rank and newrank!=self.rank:
                    self._stats_incrementMoteStats('rplChurnRank')
                    # log
                    self._log(
                        self.INFO,
                        "[rpl] churn: rank {0}->{1}",
                        (self.rank,newrank),
                    )

                # raise BaseException('random seed = %.5f, sf = %s, pkperiod = %s' % (self.rSeed, str(self.settings.sf), str(self.settings.pkPeriod)))

                if newPreferredParent.preferredParent == self:
                    for (neighbor,neighborRank) in self.neighborRank.items():
                        rankIncrease = self._rpl_calcRankIncrease(neighbor)
                        # print 'On mote %d, neighbor %d has rank %d and rankincrease %d and estimated ETX %.8f' % (self.id, neighbor.id, neighborRank, rankIncrease, self._estimateETX(neighbor))
                    print 'On mote %d, the rank increase to the new pref parent is: %.8f' % (self.id, self._estimateETX(newPreferredParent))
                    print 'On mote %d, the rank increase to the new pref parent is: %d' % (self.id, self._rpl_calcRankIncrease(newPreferredParent))
                    print 'On mote %d, old preferred parent: %d' % (self.id, self.preferredParent.id)
                    print 'On mote %d, just set preferred parent to %d which has %d as preferred parent.' % (self.id, newPreferredParent.id, newPreferredParent.preferredParent.id)
                    print 'On mote %d, new rank is %d' % (self.id, newrank)
                    print 'On mote %d, old rank was %d' % (self.id, self.rank)
                    print 'On mote %d, rank is %d' % (newPreferredParent.id, newPreferredParent.rank)
                    print 'On mote %d, rank is of %d is %d' % (self.id, newPreferredParent.id, self.neighborRank[newPreferredParent])
                    raise BaseException('asn = %d, seed = %.5f, sf = %s, pkperiod = %s' % (self.engine.asn, self.rSeed, str(self.settings.sf), str(self.settings.pkPeriod)))
                    # raise -1

                # self._sixtop_update_recurrent_reservation(self.preferredParent, newPreferredParent)

                # store new preferred parent and rank
                (self.preferredParent,self.rank) = (newPreferredParent,newrank)

                # calculate DAGrank
                self.dagRank = int(self.rank/self.RPL_MIN_HOP_RANK_INCREASE)

                # pick my parent set
                self.parentSet = [n for (n,_) in sorted_potentialRanks if self.neighborRank[n]<self.rank][:self.RPL_PARENT_SET_SIZE]
                assert self.preferredParent in self.parentSet

                if oldParentSet!=set([parent.id for parent in self.parentSet]):
                    self._stats_incrementMoteStats('rplChurnParentSet')

            #===
            # refresh the following parameters:
            # - self.trafficPortionPerParent

            etxs        = OrderedDict([(p, 1.0/(self.neighborRank[p]+self._rpl_calcRankIncrease(p))) for p in self.parentSet])
            sumEtxs     = float(sum(etxs.values()))
            self.trafficPortionPerParent = OrderedDict([(p, etxs[p]/sumEtxs) for p in self.parentSet])

            # remove TX cells to neighbor who are not in parent set
            for neighbor in self.numCellsToNeighbors.keys():
                # if neighbor not in self.parentSet: # NOTE: because of the downwards traffic, add the extra condition that the neighbor has to be in the oldParentSet
                # if self.parentSet == None:
                #     raise BaseException('parentSet is None')
                # if oldParentSet == None:
                #     raise BaseException('old parent set is None')
                if neighbor not in self.parentSet and (oldParentSet != None and neighbor in oldParentSet):

                    # log
                    self._log(
                        self.INFO,
                        "[otf] removing cell to {0}, since not in parentSet {1}",
                        (neighbor.id,[p.id for p in self.parentSet]),
                    )

                    selfSchedule = self.schedule
                    # make sure that 6P reservations only happen in the normal frames
                    if self.settings.switchFrameLengths:
                        selfSchedule = self.defaultSchedule

                    tsList=[ts for ts, cell in selfSchedule.iteritems() if cell['neighbor']==neighbor and cell['dir']==self.DIR_TX]
                    # print 'On mote %d, in RPL housekeeping. Removing cells %s to %d, since not in parent set.' % (self.id, tsList, neighbor.id)

                    # because now, there is also downwards traffic, you have to watch out with just deleting all cells.
                    # id est, cells that were explicitly reserved to a child should not be deleted
                    self._sixtop_cell_deletion_sender(neighbor,tsList)

    def _rpl_calcRankIncrease(self, neighbor):

        with self.dataLock:

            # estimate the ETX to that neighbor
            etx = self._estimateETX(neighbor)

            # return if that failed
            if not etx:
                return

            # per draft-ietf-6tisch-minimal, rank increase is (3*ETX-2)*RPL_MIN_HOP_RANK_INCREASE
            return int(((3*etx) - 2)*self.RPL_MIN_HOP_RANK_INCREASE)

    #===== otf

    def _otf_schedule_housekeeping(self, first=False):

        if first:
            self.engine.scheduleIn(
                delay       = self.bootDelay,
                cb          = self._otf_action_housekeeping,
                uniqueTag   = (self.id,'_otf_action_housekeeping'),
                priority    = 4,
            )
            print 'On mote %d, first ASN to do OTF housekeeping will be in %d seconds.' % (self.id, self.bootDelay)
        else:
            self.engine.scheduleIn(
                delay       = self.otfHousekeepingPeriod*(0.9+0.2*self.randomGen.random()),
                cb          = self._otf_action_housekeeping,
                uniqueTag   = (self.id,'_otf_action_housekeeping'),
                priority    = 4,
            )

    def _otf_action_child_housekeeping(self):
        '''
        OTF algorithm: decides when to add/delete TX cells to children.
        '''
        with self.dataLock:
            assert self.settings.sixtopInTXRX and self.settings.sixtopInSHARED
            # calculate the "moving average" incoming traffic, in pkts since last cycle, per neighbor

            # log
            self._log(
                self.INFO,
                "[otf - child housekeeping] Doing child housekeeping."
            )

            # if self.engine.asn > 10000:
            #     raise BaseException('lolol')

            toNeighbors = []
            # collect all neighbors I have TX cells to
            # NOTE: this won't work because there will never be TX cells to children before this otf_action_child_housekeeping is executed...
            # toNeighbors = [cell['neighbor'] for (ts,cell) in self.schedule.items() if cell['dir']==self.DIR_TX]
            # NOTE: should work with the neighbors in the traffic loggers.
            if len(self.inTrafficTo) > 0:
                for nb in self.inTrafficTo.keys():
                    toNeighbors += [nb]

                # remove duplicates
                toNeighbors = list(set(toNeighbors))
                toNeighbors = sorted(toNeighbors, key=lambda x: x.id, reverse=True)
                # remove parent, because we are only considering children
                if self.preferredParent in toNeighbors: toNeighbors.remove(self.preferredParent)
                toChildNeighbors = toNeighbors

                # reset inTrafficMovingAveTo
                childNeighborMovingAve = self.inTrafficMovingAveTo.keys()
                for childNeighbor in childNeighborMovingAve:
                    if childNeighbor not in toChildNeighbors: # so, no traffic from this child neighbor
                        del self.inTrafficMovingAveTo[childNeighbor]

                # set inTrafficMovingAveTo
                for childNeighbor in toChildNeighbors:
                    if childNeighbor in self.inTrafficMovingAveTo:
                        newTraffic   = 0
                        newTraffic  += self.inTrafficTo[childNeighbor]*self.OTF_TRAFFIC_SMOOTHING               # new
                        newTraffic  += self.inTrafficMovingAveTo[childNeighbor]*(1-self.OTF_TRAFFIC_SMOOTHING)  # old
                        self.inTrafficMovingAveTo[childNeighbor] = newTraffic
                    elif self.inTrafficTo[childNeighbor] != 0: # last time we did not get traffic from this child: it is new
                        self.inTrafficMovingAveTo[childNeighbor] = self.inTrafficTo[childNeighbor]

                # sumInTrafficMovingAveTo = float(sum(self.inTrafficMovingAveTo.values()))
                # self.trafficPortionPerRxNeighbor = dict([(childNeighbor, self.inTrafficMovingAveTo[childNeighbor]/sumInTrafficMovingAveTo) for childNeighbor in self.inTrafficMovingAveTo])

                # etxs        = dict([(childNeighbor, 1.0/(self.neighborRank[childNeighbor]+self._rpl_calcRankIncrease(childNeighbor))) for p in toChildNeighbors])
                # TODO: just use estimateETX, why use the whole rank system if it is the etx what you want to calculate? Especially for the children.
                # PLUS, the neighborRank[childNeighbor] gives a KeyError, probably b/c the rank is not yet known: not yet received a DIO?
                etxs        = OrderedDict([(childNeighbor, 1.0/self._estimateETX(childNeighbor)) for childNeighbor in toChildNeighbors])
                sumEtxs     = float(sum(etxs.values()))
                portionPerChildNeighbor = OrderedDict([(childNeighbor, etxs[childNeighbor]/sumEtxs) for childNeighbor in toChildNeighbors])

                # go for every to child neighbor
                for toChild in self.inTrafficMovingAveTo.keys():

                    # calculate my total generated traffic, in pkt/s
                    genTraffic       = 0

                    effectiveTXPeriod = self.otfHousekeepingPeriod # the period in which the mote could actually generate and receive data

                    # generated/relayed by me
                    genTraffic  += self.inTrafficMovingAveTo[toChild]/effectiveTXPeriod

                    # convert to pkts/cycle
                    genTraffic      *= self.settings.slotframeLength*self.settings.slotDuration

                    # calculate required number of cells to that parent
                    etx = self._estimateETX(toChild)
                    if etx > self.RPL_MAX_ETX: # cap ETX
                        etx  = self.RPL_MAX_ETX
                    reqCells      = int(math.ceil(genTraffic*etx))

                    # calculate the OTF threshold
                    threshold     = int(math.ceil(portionPerChildNeighbor[toChild]*self.settings.otfThreshold))

                    # measure how many cells I have now to that parent
                    nowCells      = self.numCellsToNeighbors.get(toChild,0)

                    if nowCells == 0 or nowCells < reqCells:
                        # I don't have enough cells

                        # calculate how many to add
                        if reqCells > 0:
                            # according to traffic
                            numCellsToAdd = reqCells - nowCells + (threshold + 1) / 2
                        else:
                            # but at least one cell
                            numCellsToAdd = 0

                        # log
                        self._log(
                            self.INFO,
                            "[otf - child housekeeping] not enough cells to child {0}: have {1}, need {2}, add {3}",
                            (toChild.id,nowCells,reqCells,numCellsToAdd),
                        )

                        # update mote stats
                        self._stats_incrementMoteStats('otfAdd')

                        # have 6top add cells
                        self._sixtop_cell_reservation_request(toChild,numCellsToAdd)

                    elif reqCells < nowCells-threshold:
                        # I have too many cells

                        numCellsToRemove = 0
                        if reqCells >= 0: # always keep one cell

                            # calculate how many to remove
                            numCellsToRemove = nowCells - reqCells

                            # log
                            self._log(
                                self.INFO,
                                "[otf - child housekeeping] too many cells to child {0}:  have {1}, need {2}, remove {3}",
                                (toChild.id,nowCells,reqCells,numCellsToRemove),
                            )

                            # update mote stats
                            self._stats_incrementMoteStats('otfRemove')

                            self._log(
                                self.INFO,
                                "[tsch - child housekeeping] On mote {0}, I have {1} cells to {2}, I want to remove {3} cells.",
                                (self.id, nowCells, toChild.id, numCellsToRemove),
                            )
                            # have 6top remove cells
                            self._sixtop_removeCells(toChild,numCellsToRemove)
                    else:
                        # nothing to do
                        pass


    def _otf_action_housekeeping(self):
        '''
        OTF algorithm: decides when to add/delete cells.
        '''

        with self.dataLock:

            # only do housekeeping for TX to children if both are enabled
            if self.settings.sixtopInSHARED and self.settings.sixtopInTXRX:
                # do the child housekeeping first, before the reset of the traffic counters (of course you could also move the reset)
                self._otf_action_child_housekeeping()

            # print 'On mote %d, enter housekeeping.' % self.id

            # calculate the "moving average" incoming traffic, in pkts since last cycle, per neighbor

            # print 'On mote %d, recurrent timeslotes in next buffer period: %s.' % (self.id, self._sixtop_cell_get_all_recurrent_timeslots())
            # collect all neighbors I have RX cells to
            # neglect the llsfcells
            rxNeighbors = [cell['neighbor'] for (ts,cell) in self.schedule.items() if cell['dir']==self.DIR_RX and not cell['llsfCell']]

            # remove duplicates
            rxNeighbors = list(set(rxNeighbors))
            rxNeighbors = sorted(rxNeighbors, key=lambda x: x.id, reverse=True)
            # reset inTrafficMovingAve
            neighbors = self.inTrafficMovingAve.keys()
            for neighbor in neighbors:
                if neighbor not in rxNeighbors:
                    del self.inTrafficMovingAve[neighbor]

            # set inTrafficMovingAve
            for neighborOrMe in rxNeighbors+[self]:
                if neighborOrMe in self.inTrafficMovingAve:
                    newTraffic   = 0
                    newTraffic  += self.inTraffic[neighborOrMe]*self.OTF_TRAFFIC_SMOOTHING               # new
                    newTraffic  += self.inTrafficMovingAve[neighborOrMe]*(1-self.OTF_TRAFFIC_SMOOTHING)  # old
                    self.inTrafficMovingAve[neighborOrMe] = newTraffic
                elif self.inTraffic[neighborOrMe] != 0:
                    self.inTrafficMovingAve[neighborOrMe] = self.inTraffic[neighborOrMe]

            # reset the incoming traffic statistics, so they can build up until next housekeeping
            self._otf_resetInboundTrafficCounters()

            # calculate my total generated traffic, in pkt/s
            genTraffic       = 0

            # NOTE: when using a mgmt frame, one has to substract the total number of shared cells that also happended since the last housekeeping
            effectiveTXPeriod = self.otfHousekeepingPeriod # the period in which the mote could actually generate and receive data
            # TODO: shouldn't this also be applied for the genaral case (with the one active cells), without the extra management frame
            if self.settings.switchFrameLengths: # for the mode with mgmt frames
                # print 'SUBSTRACTING %s SLOTS' % (str(self.mgmtSlotsToSubstract))
                effectiveTXPeriod -= self.mgmtSlotsToSubstract * self.settings.slotDuration

            # generated/relayed by me
            for neighborOrMe in self.inTrafficMovingAve:
                # genTraffic  += self.inTrafficMovingAve[neighborOrMe]/self.otfHousekeepingPeriod
                genTraffic  += self.inTrafficMovingAve[neighborOrMe]/effectiveTXPeriod

            # NOTE: for the switching frames (mgmt frame) these params can stay the same.
            # convert to pkts/cycle
            genTraffic      *= self.settings.slotframeLength*self.settings.slotDuration

            remainingPortion = 0.0
            parent_portion   = self.trafficPortionPerParent.items()

            # TODO: only allow 1 parent, in a later version this should be changed
            assert len(parent_portion) <= 1

            # sort list so that the parent assigned larger traffic can be checked first
            sorted_parent_portion = sorted(parent_portion, key = lambda x: x[1], reverse=True)

            # split genTraffic across parents, trigger 6top to add/delete cells accordingly
            for (parent,portion) in sorted_parent_portion:

                # if some portion is remaining, this is added to this parent
                if remainingPortion!=0.0:
                    portion                               += remainingPortion
                    remainingPortion                       = 0.0
                    self.trafficPortionPerParent[parent]   = portion

                # calculate required number of cells to that parent
                etx = self._estimateETX(parent)
                if etx>self.RPL_MAX_ETX: # cap ETX
                    etx  = self.RPL_MAX_ETX

                # countLLSFPackets = 0
                extraCells = 0
                if (self.settings.sf == 'recurrent' or  self.settings.sf == 'recurrent_chang'):
                    # for pkt in self.txQueue:
                    #     if pkt['type'] == self.APP_TYPE_DATA and pkt['payload'][3] != None:
                    #         reqCells += 1
                    if self.id != 0:
                        extraCells = self.getAveragePacketsInQueue()
                        self.resetPacketsInQueue()

                reqCells      = int(math.ceil(portion*(extraCells + genTraffic)*etx))

                self._log(
                    self.INFO,
                    "[tsch - housekeeping] On mote {0}, portion = {1}, genTraffic = {2}, etx = {3} and reqCell = {4}.",
                    (self.id, portion, genTraffic, etx, reqCells),
                )
                # print 'On mote %d, portion = %.2f, genTraffic = %.2f, etx = %.2f and reqCell = %.2f.' % (self.id, portion, genTraffic, etx, reqCells)

                # calculate the OTF threshold
                threshold     = int(math.ceil(portion*self.settings.otfThreshold))
                # print 'on mote %d, the threshold is %d' % (self.id, threshold)

                # measure how many cells I have now to that parent
                nowCells      = self.numCellsToNeighbors.get(parent,0)
                # print 'on mote %d, the nowCells is %d' % (self.id, nowCells)
                # print 'on mote %d, the reqCells is %d' % (self.id, reqCells)

                # if self.id == 1 and self.engine.asn == 18649:
                #     reqCells = 2

                if nowCells==0 or nowCells<reqCells:
                    # I don't have enough cells

                    # calculate how many to add
                    if reqCells>0:
                        # according to
                        numCellsToAdd = reqCells-nowCells+(threshold+1)/2
                    else:
                        # but at least one
                        numCellsToAdd = 1

                    # log
                    self._log(
                        self.INFO,
                        "[otf - housekeeping] not enough cells to {0}: have {1}, need {2}, add {3}",
                        (parent.id,nowCells,reqCells,numCellsToAdd),
                    )

                    # update mote stats
                    self._stats_incrementMoteStats('otfAdd')

                    # have 6top add cells
                    self._sixtop_cell_reservation_request(parent,numCellsToAdd)

                    # TODO: this has to be re-enabled when using multiple parents (probably also moved to another place)
                    # As there is only one parent, and the trafficPortionPerParent is not used anywhere else (it is set in the rpl_housekeeping, but not used there), it should be safe to comment this
                    #
                    # # measure how many cells I have now to that parent
                    # nowCells     = self.numCellsToNeighbors.get(parent,0)
                    #
                    # # store handled portion and remaining portion
                    # if nowCells<reqCells:
                    #     handledPortion   = (float(nowCells)/etx)/genTraffic
                    #     remainingPortion = portion - handledPortion
                    #     self.trafficPortionPerParent[parent] = handledPortion

                    # remember OTF triggered
                    otfTriggered = True

                elif reqCells<nowCells-threshold:
                    # I have too many cells

                    numCellsToRemove = 0
                    if reqCells > 0: # always keep one cell

                        # calculate how many to remove
                        numCellsToRemove = nowCells-reqCells

                        # log
                        self._log(
                            self.INFO,
                            "[otf -  housekeeping] too many cells to {0}:  have {1}, need {2}, remove {3}",
                            (parent.id,nowCells,reqCells,numCellsToRemove),
                        )

                        # update mote stats
                        self._stats_incrementMoteStats('otfRemove')

                        self._log(
                            self.INFO,
                            "[tsch - housekeeping] On mote {0}, I have {1} cells to {2}, I want to remove {3} cells.",
                            (self.id, nowCells, parent.id, numCellsToRemove),
                        )
                        # raise -1
                        # print 'On mote %d, I have %d cells to %d, I want to remove %d cells.' % (self.id, nowCells, parent.id, numCellsToRemove)

                        # have 6top remove cells
                        self._sixtop_removeCells(parent,numCellsToRemove)

                    # else:
                        # print 'On mote %d, eventually did NOT remove cells, because I have %d cells to %d and I want to remove %d cells which would make the total 0.' % (self.id, nowCells, parent.id, numCellsToRemove)
                    # remember OTF triggered
                    otfTriggered = True

                else:
                    # nothing to do

                    # remember OTF did NOT trigger
                    otfTriggered = False

                # maintain stats
                if otfTriggered:
                    now = self.engine.getAsn()
                    if not self.asnOTFevent:
                        assert not self.timeBetweenOTFevents
                    else:
                        self.timeBetweenOTFevents += [now-self.asnOTFevent]
                    self.asnOTFevent = now

            self.mgmtSlotsToSubstract = 0 # set back to zero. From this moment until the next houskeeping, log the shared cells so they can be substracted.

            # schedule next housekeeping
            self._otf_schedule_housekeeping()

    def _otf_resetInboundTrafficCounters(self):
        with self.dataLock:
            for neighbor in self._myNeigbors()+[self]:
                self.inTraffic[neighbor] = 0
                self.inTrafficLLSF[neighbor] = 0
            self.inTrafficTo = OrderedDict() # important for the child housekeeping that is is set like this! otherwise you introduce neighbors that originally were not in this variable

    def _otf_incrementIncomingTraffic(self,neighbor):
        with self.dataLock:
            self.inTraffic[neighbor] += 1

    def _otf_incrementIncomingTrafficForChild(self, to):
        with self.dataLock:
            if to not in self.inTrafficTo:
                self.inTrafficTo[to] = 0
            self.inTrafficTo[to] += 1

    def _otf_incrementIncomingLLSFTraffic(self,neighbor):
        with self.dataLock:
            self.inTrafficLLSF[neighbor] += 1

    #===== 6top

    def _sixtop_update_llsf_reservation(self, llsfUniqueID_par):
        with self.dataLock:
            # print 'On mote %d, doing an update for ID %s.' % (self.id, llsfUniqueID_par)
            timeslot = 0
            interval = int((float(10.0)/float(self.settings.slotDuration)))
            startASN = 43957
            endASN = 101000
            reservationTuple = (timeslot, interval, startASN, endASN)
            transID_outgoing = self._sixtop_cell_llsf_reservation_request(self.preferredParent, reservationTuple, llsfUniqueID=llsfUniqueID_par)

            transID_incoming = None
            # look for the tuple with the correct outgoing transID
            toRemoveTuples = []
            for llsf_id, listTuples in self.llsfTransIDs.iteritems():
                for IDsTuple in listTuples:
                    if llsf_id == llsfUniqueID_par:
                        transID_incoming = IDsTuple[0] # the incoming transID
                        toRemoveTuples.append(IDsTuple)
                        break
                if transID_incoming != None: # if you already found it, break
                    break

            # print 'Before renewing of transids: %s' % self.llsfTransIDs[llsfUniqueID_par]

            # remove it
            for tup in toRemoveTuples:
                self.llsfTransIDs[llsfUniqueID_par].remove(tup)

            ## reservation successful?
            if not transID_outgoing: # no success
                timerValue = self.sixtopTransactionTX[self.preferredParent][3] + 1
                newASN = self.engine.asn + timerValue
                self.delayedLLSFRequests.append((newASN, transID_incoming, neighborID, reservationTuple, llsfUniqueID_par, numCellsOrig))
            elif self.tagTransID == self.RESERVATION_NOTFEASABLE:
                pass
            else: # yes success
                # add it to transID map
                if llsfUniqueID_par not in self.llsfTransIDs:
                    self.llsfTransIDs[llsfUniqueID_par] = []
                # add the new outgoing id
                self.llsfTransIDs[llsfUniqueID_par].append((transID_incoming, transID_outgoing))
                ######  End - This piece of code makes sure that when a llsf request fails, a new gets send, and the transIDs get replaced ######

            # print 'After renewing of transids: %s' % self.llsfTransIDs[llsfUniqueID_par]

    def _sixtop_remove_llsf_reservation(self, llsfUniqueID_par):
        ''' Remove a llsf reservation (in the TX and/or RX tuples) based on the x ID. '''
        with self.dataLock:
            # raise -1

            toRemoveRXBlocked = []
            for reservationRXBlockedTuple in self.blockedLLSFReservationsRXTuples:
                ch = reservationRXBlockedTuple[1]
                interval = reservationRXBlockedTuple[2]
                startASN = reservationRXBlockedTuple[3]
                endASN = reservationRXBlockedTuple[4]
                transID = reservationRXBlockedTuple[5]
                neighbor = reservationRXBlockedTuple[6]
                llsfUniqueID = reservationRXBlockedTuple[7]

                if llsfUniqueID == llsfUniqueID_par:
                    toRemoveRXBlocked.append(reservationRXBlockedTuple)
                    # break

            if toRemoveRXBlocked != []:
                for rx in toRemoveRXBlocked:
                    self.blockedLLSFReservationsRXTuples.remove(rx)
                    # remove the tuple from the recurrent RX reservations
                    self._log(
                        self.INFO,
                        "[sixtop] On mote {0}, removed recurrent RX blocked reservation with ID {1}.",
                        (self.id, llsfUniqueID_par),
                    )

            toRemoveRX = []
            for reservationRXTuple in self.LLSFReservationsRXTuples:
                ch = reservationRXTuple[1]
                interval = reservationRXTuple[2]
                startASN = reservationRXTuple[3]
                endASN = reservationRXTuple[4]
                transID = reservationRXTuple[5]
                neighbor = reservationRXTuple[6]
                llsfUniqueID = reservationRXTuple[7]

                if llsfUniqueID == llsfUniqueID_par:
                    toRemoveRX.append(reservationRXTuple)
                    # break

            if toRemoveRX != []:
                for rx in toRemoveRX:
                    self.LLSFReservationsRXTuples.remove(rx)
                    # remove the tuple from the recurrent RX reservations
                    self._log(
                        self.INFO,
                        "[sixtop] On mote {0}, removed recurrent RX reservation with ID {1}.",
                        (self.id, llsfUniqueID_par),
                    )

            toRemoveTXBlocked = []
            for reservationTXBlockedTuple in self.blockedLLSFReservationsTXTuples:
                ch = reservationTXBlockedTuple[1]
                interval = reservationTXBlockedTuple[2]
                startASN = reservationTXBlockedTuple[3]
                endASN = reservationTXBlockedTuple[4]
                transID = reservationTXBlockedTuple[5]
                neighbor = reservationTXBlockedTuple[6]
                llsfUniqueID = reservationTXBlockedTuple[7]

                if llsfUniqueID == llsfUniqueID_par:
                    toRemoveTXBlocked.append(reservationTXBlockedTuple)
                    # break

            if toRemoveTXBlocked != []:
                for tx in toRemoveTXBlocked:
                    self.blockedLLSFReservationsTXTuples.remove(tx)
                    # remove the tuple from the recurrent RX reservations
                    self._log(
                        self.INFO,
                        "[sixtop] On mote {0}, removed recurrent TX blocked reservation with ID {1}.",
                        (self.id, llsfUniqueID_par),
                    )

            toRemoveTX = []
            for reservationTXTuple in self.LLSFReservationsTXTuples:
                ch = reservationTXTuple[1]
                interval = reservationTXTuple[2]
                startASN = reservationTXTuple[3]
                endASN = reservationTXTuple[4]
                transID = reservationTXTuple[5]
                neighbor = reservationTXTuple[6]
                llsfUniqueID = reservationTXTuple[7]

                if llsfUniqueID == llsfUniqueID_par:
                    toRemoveTX.append(reservationTXTuple)
                    # break

            if toRemoveTX != []:
                for tx in toRemoveTX:
                    self.LLSFReservationsTXTuples.remove(tx)
                    # remove the tuple from the recurrent RX reservations
                    self._log(
                        self.INFO,
                        "[sixtop] On mote {0}, removed recurrent TX reservation with ID {1}.",
                        (self.id, llsfUniqueID_par),
                    )
            # remove the tuple from the recurrent RX reservations

    def _sixtop_remove_llsf_reservation_with_prefix(self, llsfUniqueID_prefix, exceptLLSFUniqueID, dir='TXRX'):
        ''' Remove a llsf reservation (in the TX and/or RX tuples) based on the x ID. '''
        with self.dataLock:
            # raise -1

            IDList = []
            if 'RX' in dir:
                toRemoveRXBlocked = []
                for reservationRXBlockedTuple in self.blockedLLSFReservationsRXTuples:
                    ch = reservationRXBlockedTuple[1]
                    interval = reservationRXBlockedTuple[2]
                    startASN = reservationRXBlockedTuple[3]
                    endASN = reservationRXBlockedTuple[4]
                    transID = reservationRXBlockedTuple[5]
                    neighbor = reservationRXBlockedTuple[6]
                    llsfUniqueID = reservationRXBlockedTuple[7]

                    if llsfUniqueID_prefix in llsfUniqueID and llsfUniqueID != exceptLLSFUniqueID:
                        toRemoveRXBlocked.append(reservationRXBlockedTuple)
                        # break

                if toRemoveRXBlocked != []:
                    for rx in toRemoveRXBlocked:
                        IDList.append(rx[7])
                        self.blockedLLSFReservationsRXTuples.remove(rx)
                        # remove the tuple from the recurrent RX reservations
                        self._log(
                            self.INFO,
                            "[sixtop] On mote {0}, removed recurrent RX blocked reservation {1} with prefix {2} (and not equal to {3}).",
                            (self.id, rx[7], llsfUniqueID_prefix, exceptLLSFUniqueID),
                        )

                toRemoveRX = []
                for reservationRXTuple in self.LLSFReservationsRXTuples:
                    ch = reservationRXTuple[1]
                    interval = reservationRXTuple[2]
                    startASN = reservationRXTuple[3]
                    endASN = reservationRXTuple[4]
                    transID = reservationRXTuple[5]
                    neighbor = reservationRXTuple[6]
                    llsfUniqueID = reservationRXTuple[7]

                    if llsfUniqueID_prefix in llsfUniqueID and llsfUniqueID != exceptLLSFUniqueID:
                        toRemoveRX.append(reservationRXTuple)
                        # break

                if toRemoveRX != []:
                    for rx in toRemoveRX:
                        IDList.append(rx[7])
                        self.LLSFReservationsRXTuples.remove(rx)
                        # remove the tuple from the recurrent RX reservations
                        self._log(
                            self.INFO,
                            "[sixtop] On mote {0}, removed recurrent RX reservation {1} with prefix {2} (and not equal to {3}).",
                            (self.id, rx[7], llsfUniqueID_prefix, exceptLLSFUniqueID),
                        )

            if 'TX' in dir:
                toRemoveTXBlocked = []
                for reservationTXBlockedTuple in self.blockedLLSFReservationsTXTuples:
                    ch = reservationTXBlockedTuple[1]
                    interval = reservationTXBlockedTuple[2]
                    startASN = reservationTXBlockedTuple[3]
                    endASN = reservationTXBlockedTuple[4]
                    transID = reservationTXBlockedTuple[5]
                    neighbor = reservationTXBlockedTuple[6]
                    llsfUniqueID = reservationTXBlockedTuple[7]

                    if llsfUniqueID_prefix in llsfUniqueID and llsfUniqueID != exceptLLSFUniqueID:
                        toRemoveTXBlocked.append(reservationTXBlockedTuple)
                        # break

                if toRemoveTXBlocked != []:
                    for tx in toRemoveTXBlocked:
                        IDList.append(tx[7])
                        self.blockedLLSFReservationsTXTuples.remove(tx)
                        # remove the tuple from the recurrent RX reservations
                        self._log(
                            self.INFO,
                            "[sixtop] On mote {0}, removed recurrent TX blocked reservation {1} with prefix {2} (and not equal to {3}).",
                            (self.id, tx[7], llsfUniqueID_prefix, exceptLLSFUniqueID),
                        )

                toRemoveTX = []
                for reservationTXTuple in self.LLSFReservationsTXTuples:
                    ch = reservationTXTuple[1]
                    interval = reservationTXTuple[2]
                    startASN = reservationTXTuple[3]
                    endASN = reservationTXTuple[4]
                    transID = reservationTXTuple[5]
                    neighbor = reservationTXTuple[6]
                    llsfUniqueID = reservationTXTuple[7]

                    if llsfUniqueID_prefix in llsfUniqueID and llsfUniqueID != exceptLLSFUniqueID:
                        toRemoveTX.append(reservationTXTuple)
                        # break

                if toRemoveTX != []:
                    for tx in toRemoveTX:
                        IDList.append(tx[7])
                        self.LLSFReservationsTXTuples.remove(tx)
                        # remove the tuple from the recurrent RX reservations
                        self._log(
                            self.INFO,
                            "[sixtop] On mote {0}, removed recurrent TX reservation {1} with prefix {2} (and not equal to {3}).",
                            (self.id, tx[7], llsfUniqueID_prefix, exceptLLSFUniqueID),
                        )
            # remove the tuple from the recurrent RX reservations
            return IDList

    def _sixtop_check_delayed_reservations(self):
        with self.dataLock:
            toRemoveRes = []
            for delayedTuple in self.delayedLLSFRequests:
                ASN = delayedTuple[0]
                transID_incoming = delayedTuple[1]
                neighborID = delayedTuple[2] # This does not get used, because you will always send your llsf reservations to your pref parent
                resTuple = delayedTuple[3]
                llsfUniqueID = delayedTuple[4]
                numCellsOrig = delayedTuple[5]
                if ASN == self.engine.asn:
                    self._log(
                        self.INFO,
                        "[sixtop] Trying a delayed recurrent request (unique id: {0}, reservation tuple: {1}).",
                        (llsfUniqueID, resTuple),
                    )
                    transID_outgoing = self._sixtop_cell_llsf_reservation_request(self.preferredParent, resTuple, llsfUniqueID=llsfUniqueID, numCellsOrig=numCellsOrig)
                    if not transID_outgoing:
                        # add again to delayedLLSFRequests
                        # the timer value, add one to only do the reservation one ASN after the current transaction
                        timerValue = self.sixtopTransactionTX[self.preferredParent][3] + 1
                        newASN = self.engine.asn + timerValue
                        self.delayedLLSFRequests.append((newASN, transID_incoming, self.preferredParent.id, resTuple, llsfUniqueID, numCellsOrig))
                        # self.delayedLLSFRequests.append((newASN, transID_incoming, neighborID, resTuple, llsfUniqueID, numCellsOrig))
                        # print 'On mote %d (in delayed requests check), added reservationTuple (%s) and transID_incoming %s coming from neighborID %d to the delayed requests, trying again at ASN %d' % (self.id, str(resTuple), transID_incoming, neighborID, newASN)
                        self._log(
                            self.INFO,
                            "[sixtop] On mote {0}, added reservationTuple ({1}) and transID_incoming {2} coming from neighborID {3} to the delayed requests, trying again at ASN {4} (llsfUniqueID = {5}, numCellsOrig = {6}).",
                            (self.id, str(resTuple), transID_incoming, neighborID, newASN, llsfUniqueID, numCellsOrig),
                        )
                    elif self.tagTransID == self.RESERVATION_NOTFEASABLE:
                        pass
                    else:
                        # add it to transID map
                        if llsfUniqueID not in self.llsfTransIDs:
                            self.llsfTransIDs[llsfUniqueID] = [] # NOTE: yes this has to be neighborID AND I have to check on preferredParent
                        # add the new outgoing id
                        self.llsfTransIDs[llsfUniqueID].append((transID_incoming, transID_outgoing))
                    toRemoveRes.append(delayedTuple)

            # remove the handled delayed tuples
            for toRemove in toRemoveRes:
                self.delayedLLSFRequests.remove(toRemove)

    def _sixtop_decrease_timers(self):
        for mote in self.sixtopTransactionTX.keys():
            if self.sixtopTransactionTX[mote][3] > 0: # the timer did not ran out yet
                if self.sixtopTransactionTX[mote][4]: # if it is llsf reservation
                    self.sixtopTransactionTX[mote] = (self.sixtopTransactionTX[mote][0], self.sixtopTransactionTX[mote][1], self.sixtopTransactionTX[mote][2], self.sixtopTransactionTX[mote][3]-1, self.sixtopTransactionTX[mote][4], self.sixtopTransactionTX[mote][5], self.sixtopTransactionTX[mote][6], self.sixtopTransactionTX[mote][7])
                else:
                    self.sixtopTransactionTX[mote] = (self.sixtopTransactionTX[mote][0], self.sixtopTransactionTX[mote][1], self.sixtopTransactionTX[mote][2], self.sixtopTransactionTX[mote][3]-1, self.sixtopTransactionTX[mote][4])
            if  self.sixtopTransactionTX[mote][3] == 0: # the timer ran out at this ASN
                # clean queue from time-outed requests
                pktToRemove = None
                for pkt in self.txQueue:
                    if pkt['type'] == self.SIXTOP_TYPE_ADD and pkt['payload'][3] == self.sixtopTransactionTX[mote][0]:
                        pktToRemove = pkt
                        break
                if pktToRemove != None:
                    self.txQueue.remove(pktToRemove)
                # print 'At mote %d, removed sixtop transaction (transID %s) because it ran out at ASN %d.' % (self.id, self.sixtopTransactionTX[mote][0], self.engine.asn)
                self._log(
                    self.INFO,
                    "[sixtop] At mote {0}, removed sixtop transaction (transID {1}) because it ran out at ASN {2}.",
                    (self.id, self.sixtopTransactionTX[mote][0], self.engine.asn),
                )
                # also remove the blocked LLSF cells because the timer ran out, based on the transID
                # only if it is a llsf transaction
                if (self.settings.sf == 'recurrent' or  self.settings.sf == 'recurrent_chang') and self.sixtopTransactionTX[mote][4]:
                    self._llsf_remove_blocked_reservationsTX(self.sixtopTransactionTX[mote][6]) # 6 is the llsfUniqueID

                    ######  Begin - This piece of code makes sure that when a llsf request fails, a new gets send, and the transIDs get replaced ######

                    # get the information for the new try
                    timeslot = self.sixtopTransactionTX[mote][5][0]
                    interval = self.sixtopTransactionTX[mote][5][1]
                    startASN = self.sixtopTransactionTX[mote][5][2]
                    endASN = self.sixtopTransactionTX[mote][5][3]
                    reservationTuple = (timeslot, interval, startASN, endASN)
                    llsfUniqueID = self.sixtopTransactionTX[mote][6]
                    numCellsOrig = self.sixtopTransactionTX[mote][7]

                    self._sixtop_resend_LLSF_reservation_immediately(reservationTuple, mote, llsfUniqueID, numCellsOrig)
                else:
                    self._tsch_releaseCells(self.sixtopTransactionTX[mote][1])
                    del self.sixtopTransactionTX[mote]

        for mote in self.sixtopDeletionTX.keys():
            if self.sixtopDeletionTX[mote][2] > 0:
                self.sixtopDeletionTX[mote] = (self.sixtopDeletionTX[mote][0], self.sixtopDeletionTX[mote][1], self.sixtopDeletionTX[mote][2]-1)
            if self.sixtopDeletionTX[mote][2] == 0:
                # clean queue from timeouted delete requests
                pktToRemove = None
                for pkt in self.txQueue:
                    if pkt['type'] == self.SIXTOP_TYPE_DELETE and pkt['payload'][2] == self.sixtopDeletionTX[mote][0]:
                        pktToRemove = pkt
                        break
                if pktToRemove != None:
                    self.txQueue.remove(pktToRemove)
                self._log(
                    self.INFO,
                    "[sixtop] At mote {0}, removed RESPONSE sixtop transaction (transID {1}) because it ran out at ASN {2}.",
                    (self.id, self.sixtopDeletionTX[mote][0], self.engine.asn),
                )
                del self.sixtopDeletionTX[mote]

    def _sixtop_decrease_timers_responses(self):
        for mote in self.sixtopTransactionTXResponses.keys():
            if self.sixtopTransactionTXResponses[mote][3] > 0:
                # transID, cells, dir, timervalue, llsfOrNot, llsfUniqueID
                if self.sixtopTransactionTXResponses[mote][4]: # if it is llsf
                    self.sixtopTransactionTXResponses[mote] = (self.sixtopTransactionTXResponses[mote][0], self.sixtopTransactionTXResponses[mote][1], self.sixtopTransactionTXResponses[mote][2], self.sixtopTransactionTXResponses[mote][3]-1, self.sixtopTransactionTXResponses[mote][4], self.sixtopTransactionTXResponses[mote][5])
                else: # not a llsf reservation
                    self.sixtopTransactionTXResponses[mote] = (self.sixtopTransactionTXResponses[mote][0], self.sixtopTransactionTXResponses[mote][1], self.sixtopTransactionTXResponses[mote][2], self.sixtopTransactionTXResponses[mote][3]-1, self.sixtopTransactionTXResponses[mote][4])
            if self.sixtopTransactionTXResponses[mote][3] == 0:
                # clean queue from time-outed requests
                pktToRemove = None
                for pkt in self.txQueue:
                    if pkt['type'] == self.SIXTOP_TYPE_RESPONSE and pkt['payload'][2] == self.sixtopTransactionTXResponses[mote][0]:
                        pktToRemove = pkt
                        break
                if pktToRemove != None:
                    self.txQueue.remove(pktToRemove)
                self._log(
                    self.INFO,
                    "[sixtop] At mote {0}, removed RESPONSE sixtop transaction (transID {1}) because it ran out at ASN {2}.",
                    (self.id, self.sixtopTransactionTXResponses[mote][0], self.engine.asn),
                )
                # also remove the blocked LLSF cells because the timer ran out, based on the unique ID
                # only if it is a llsf transaction
                if (self.settings.sf == 'recurrent' or  self.settings.sf == 'recurrent_chang') and self.sixtopTransactionTXResponses[mote][4]:
                    self._llsf_remove_blocked_reservationsRX(self.sixtopTransactionTXResponses[mote][5])
                else:
                    self._tsch_releaseCells(self.sixtopTransactionTXResponses[mote][1])

                del self.sixtopTransactionTXResponses[mote]

        for mote in self.sixtopDeletionTXResponses.keys():
            if self.sixtopDeletionTXResponses[mote][2] > 0:
                self.sixtopDeletionTXResponses[mote] = (self.sixtopDeletionTXResponses[mote][0], self.sixtopDeletionTXResponses[mote][1], self.sixtopDeletionTXResponses[mote][2]-1)
            if self.sixtopDeletionTXResponses[mote][2] == 0:
                # clean queue from timeouted delete requests
                pktToRemove = None
                for pkt in self.txQueue:
                    if pkt['type'] == self.SIXTOP_TYPE_DELETE_RESPONSE and pkt['payload'][2] == self.sixtopDeletionTXResponses[mote][0]:
                        pktToRemove = pkt
                        break
                if pktToRemove != None:
                    self.txQueue.remove(pktToRemove)
                # print 'At mote %d, removed RESPONSE sixtop deletion (deleteID %s) because it ran out at ASN %d.' % (self.id, self.sixtopDeletionTXResponses[mote][0], self.engine.asn)
                self._log(
                    self.INFO,
                    "[sixtop] At mote {0}, removed RESPONSE sixtop deletion (deleteID {1}) because it ran out at ASN {2}.",
                    (self.id, self.sixtopDeletionTXResponses[mote][0], self.engine.asn),
                )
                del self.sixtopDeletionTXResponses[mote]

    def getParentIDList(self, parentList):
        if not self.dagRoot:
            parentList.append(self.preferredParent.id)
            self.getParentIDList(parentList)

    def _sixtop_update_recurrent_reservation(self):
        ''' this method gets called when a mote changes pref parent and the recurrent reservation has to be deleted and resend '''

        # 1) first step: get the recursive preferred parent list
        parentList = []
        parentList = self.getParentIDList(parentList)

        # 2) second step: get all the llsf unique IDs
        idsTX = []
        ids = []
        for resTuple in self.LLSFReservationsRXTuples:
            ids.append(resTuple[7])
        for resTuple in self.LLSFReservationsTXTuples:
            ids.append(resTuple[7])
            idsTX.append(resTuple[7])
        for resTuple in self.blockedLLSFReservationsRXTuples:
            ids.append(resTuple[7])
        for resTuple in self.blockedLLSFReservationsTXTuples:
            ids.append(resTuple[7])
            idsTX.append(resTuple[7])

        uniqueIDs = set(ids)
        uniqueTXIDs = set(idsTX)

    def _sixtop_action_enqueue_ADD(self, cells, dir, dest=None, llsfPayload=None, numCells=None, llsfUniqueID=None, thresholds=None):
        ''' enqueue 6P ADD packet into stack. returns the ID of the transaction '''

        # destination is preferred parent
        if dest is None:
            destination = self.preferredParent
        else:
            destination = dest

        # this is 6P transaction identifier
        transID = '%s_%s_%s_%s' % (str(self.id), str(destination.id), self.SIXTOP_TYPE_ADD, (self.engine.getAsn()))
        self._log(
            self.INFO,
            "[sixtop] On mote {0} at ASN {1} ({2}), creating a new 6P ADD message (transID {3}) with llsf info {4}.",
            (str(self.id), str(self.engine.asn), self.engine.getCycle(), str(transID), str(llsfPayload)),
        )
        # create new packet
        newPacket = {
            'asn':            self.engine.getAsn(),
            'type':           self.SIXTOP_TYPE_ADD,
            'payload':        [self.rank, cells, dir, transID, llsfPayload, numCells, llsfUniqueID, thresholds], # the payload is the rpl rank
            'retriesLeft':    self.TSCH_MAXTXRETRIES, # do not retransmit broadcast
            'nextTX':         1, # SHARED SLOTS, SLOTTED ALOHA: this packet should be sent the first next possibility
            'destination':    destination, # SHARED SLOTS, the destination
        }

        if llsfPayload != None:
            for reservationTuple in llsfPayload:
                reservationASNs = self._llsf_get_reservationASNs(reservationTuple[3], reservationTuple[4], reservationTuple[2])
                assert len(reservationASNs) > 0
                self.blockedLLSFReservationsTXTuples.append((reservationTuple[0], reservationTuple[1], reservationTuple[2], reservationTuple[3], reservationTuple[4], transID, destination, llsfUniqueID)) # save the reservationTuple

        # update mote stats
        self._stats_incrementMoteStats('topGeneratedAdd')
        if llsfPayload != None:
            self._stats_incrementMoteStats('topGeneratedAddLLSF')

        # enqueue packet in TSCH queue
        isEnqueued = self._tsch_enqueue_neighbor(newPacket, neighbor=destination)

        if isEnqueued:
            self._log(
                self.INFO,
                "On mote {0} at ASN {1} ({2}), succesfully enqueued a new 6P ADD message.",
                (str(self.id), str(self.engine.asn), self.engine.getCycle()),
            )
            self._stats_incrementMoteStats('topSuccessGeneratedAdd')
            if llsfPayload != None:
                self._stats_incrementMoteStats('topSuccessGeneratedAddLLSF')

            # increment traffic
            # TODO: this can be dangerous
            if destination is self.preferredParent:
                if llsfPayload is not None:
                    self._otf_incrementIncomingTraffic(self)
                else:
                    self._otf_incrementIncomingLLSFTraffic(self)
            else:
                self._otf_incrementIncomingTrafficForChild(destination)

            # the 6P transaction identifier
            return transID
        else:
            self._log(
                self.INFO,
                "[sixtop] On mote {0} at ASN {1} ({2}), UNsuccessfully enqueued a new 6P ADD message.",
                (str(self.id), str(self.engine.asn), self.engine.getCycle()),
            )
            # update mote stats
            self._stats_incrementMoteStats('dropped6PFailedEnqueue')
            if llsfPayload != None:
                self._stats_incrementMoteStats('dropped6PFailedEnqueueLLSF')
            return False

        return False

    def _sixtop_action_enqueue_DELETE(self, tsList, neighbor):
        ''' enqueue 6P DELETE packet into stack. returns the ID of the transaction '''

        # destination is preferred parent
        # NOTE: destination is NOT always preferred parent, because if you have changed parent, the message should still arrive
        # destination = self.preferredParent
        destination = neighbor

        # this is 6P transaction identifier
        deleteID = '%s_%s_%s_%s' % (str(self.id), str(destination.id), self.SIXTOP_TYPE_DELETE, (self.engine.getAsn()))

        # print 'On mote %s at ASN %s (%d), creating a new 6P DELETE message (deleteID %s).' % (str(self.id), str(self.engine.asn), self.engine.getCycle(), str(deleteID))
        self._log(
            self.INFO,
            "[sixtop] On mote {0} at ASN {1} ({2}), creating a new 6P DELETE message (deleteID {3}).",
            (str(self.id), str(self.engine.asn), self.engine.getCycle(), str(deleteID)),
        )

        # create new packet
        newPacket = {
            'asn':            self.engine.getAsn(),
            'type':           self.SIXTOP_TYPE_DELETE,
            'payload':        [self.rank, tsList, deleteID], # the payload is the rpl rank
            'retriesLeft':    self.TSCH_MAXTXRETRIES, # do not retransmit broadcast
            'nextTX':         1, # SLOTTED ALOHA: this packet should be sent the first next possibility
            'destination':    destination,
        }

        # update mote stats
        self._stats_incrementMoteStats('topGeneratedDelete')

        # enqueue packet in TSCH queue
        isEnqueued = self._tsch_enqueue_neighbor(newPacket, neighbor=destination)

        if isEnqueued:
            # print 'On mote %s at ASN %s (%d), succesfully enqueued a new 6P DELETE message.' % (str(self.id), str(self.engine.asn), self.engine.getCycle())
            self._log(
                self.INFO,
                "[sixtop] On mote {0} at ASN {1} ({2}), succesfully enqueued a new 6P DELETE message.",
                (str(self.id), str(self.engine.asn), self.engine.getCycle()),
            )
            self._stats_incrementMoteStats('topSuccessGeneratedDelete')

            # increment traffic
            if destination is self.preferredParent:
                self._otf_incrementIncomingTraffic(self)
            else:
                self._otf_incrementIncomingTrafficForChild(destination)
        else:
            # print 'On mote %s at ASN %s (%d), UNsuccessfully enqueued a new 6P DELETE message.' % (str(self.id), str(self.engine.asn), self.engine.getCycle())
            self._log(
                self.INFO,
                "[sixtop] On mote %s at ASN %s (%d), UNsuccessfully enqueued a new 6P DELETE message.",
                (str(self.id), str(self.engine.asn), self.engine.getCycle()),
            )
            # update mote stats
            self._stats_incrementMoteStats('dropped6PFailedEnqueue')

        # the 6P delete identifier
        return deleteID

    def _sixtop_action_enqueue_DELETE_RESPONSE(self, tsList, neighbor, deleteID):
        ''' enqueue 6P DELETE RESPONSE packet into stack. returns the ID of the transaction '''

        # print 'On mote %s at ASN %s (%d), creating a new 6P DELETE RESPONSE message (deleteID %s).' % (str(self.id), str(self.engine.asn), self.engine.getCycle(), str(deleteID))
        self._log(
            self.INFO,
            "[sixtop] On mote {0} at ASN {1} ({2}), creating a new 6P DELETE RESPONSE message (deleteID {3}).",
            (str(self.id), str(self.engine.asn), self.engine.getCycle(), str(deleteID)),
        )

        if neighbor in self.sixtopDeletionTXResponses: # if there is still a deletion ongoing, delete that one, because you received a new delete message
            del self.sixtopDeletionTXResponses[neighbor]

        # calculate the timer correct value
        add_ASN = int(deleteID.split("_")[-1])
        diff = self.engine.asn - add_ASN
        timervalue = self.sixtop_timout_init - diff
        self.sixtopDeletionTXResponses[neighbor] = (deleteID, tsList, timervalue)

        pktToRemove = None
        for pkt in self.txQueue:
            # check if the packet has the correct type and if the source of the deletion is the same and if the ASN (of the deletion) is earlier
            if pkt['type'] == self.SIXTOP_TYPE_DELETE_RESPONSE and (int(pkt['payload'][2].split('_')[0]) == int(deleteID.split('_')[0])) and (int(pkt['payload'][2].split('_')[-1]) < int(deleteID.split('_')[-1])):
                pktToRemove = pkt
                break
        if pktToRemove:
            # release the cells that were reserved locally for deleting
            self.txQueue.remove(pktToRemove)

        # create new packet
        newPacket = {
            'asn':            self.engine.getAsn(),
            'type':           self.SIXTOP_TYPE_DELETE_RESPONSE,
            'payload':        [self.rank, tsList, deleteID], # the payload is the rpl rank
            'retriesLeft':    self.TSCH_MAXTXRETRIES, # do not retransmit broadcast
            'nextTX':         1, # SLOTTED ALOHA: this packet should be sent the first next possibility
            'destination':    neighbor,
        }

        # update mote stats
        self._stats_incrementMoteStats('topGeneratedDeleteResponse')

        # enqueue packet in TSCH queue
        isEnqueued = self._tsch_enqueue_neighbor(newPacket, neighbor=neighbor)

        if isEnqueued:
            # print 'On mote %s at ASN %s (%d), succesfully enqueued a new 6P DELETE RESPONSE message.' % (str(self.id), str(self.engine.asn), self.engine.getCycle())
            self._log(
                self.INFO,
                "On mote {0} at ASN {1} ({2}), succesfully enqueued a new 6P DELETE RESPONSE message.",
                (str(self.id), str(self.engine.asn), self.engine.getCycle()),
            )
            self._stats_incrementMoteStats('topSuccessGeneratedDeleteResponse')

            # increment traffic
            if neighbor is self.preferredParent:
                self._otf_incrementIncomingTraffic(self)
            else:
                self._otf_incrementIncomingTrafficForChild(neighbor)
        else:
            # print 'On mote %s at ASN %s (%d), UNsuccessfully enqueued a new 6P DELETE RESPONSE message.' % (str(self.id), str(self.engine.asn), self.engine.getCycle())
            self._log(
                self.INFO,
                "On mote %s at ASN %s (%d), UNsuccessfully enqueued a new 6P DELETE RESPONSE message.",
                (str(self.id), str(self.engine.asn), self.engine.getCycle()),
            )
            # update mote stats
            self._stats_incrementMoteStats('dropped6PFailedEnqueue')

    def _sixtop_action_enqueue_RESPONSE(self, cells, neighbor, transID, dir, llsfPayload = None, numCells=None, llsfUniqueID=None):
        ''' enqueue 6P RESPONSE packet into stack '''

        # print 'On mote %s at ASN %s, creating a new 6P RESPONSE message (transID %s).' % (str(self.id), str(self.engine.asn), str(transID))
        self._log(
            self.INFO,
            "On mote {0} at ASN {1}, creating a new 6P RESPONSE message (transID {2}).",
            (str(self.id), str(self.engine.asn), str(transID)),
        )
        # if there is still another response transaction going on, delete that
        # also clean that message from the queue
        if neighbor in self.sixtopTransactionTXResponses:
            del self.sixtopTransactionTXResponses[neighbor]

        add_ASN = int(transID.split("_")[-1])
        diff = self.engine.asn - add_ASN
        timervalue = self.sixtop_timout_init - diff
        if llsfPayload is not None:
            self.sixtopTransactionTXResponses[neighbor] = (transID, cells, dir, timervalue, True, llsfUniqueID)
        else:
            self.sixtopTransactionTXResponses[neighbor] = (transID, cells, dir, timervalue, False)

        # if there is still an earlier response in the queue from another add, remove this first
        # this is different from what the draft says (https://tools.ietf.org/html/draft-wang-6tisch-6top-sublayer-04#section-3.2.3), but it is easier
        pktToRemove = None
        for pkt in self.txQueue:
            # check if the packet has the correct type and if the source of the deletion is the same and if the ASN (of the deletion) is earlier
            if pkt['type'] == self.SIXTOP_TYPE_RESPONSE and (int(pkt['payload'][2].split('_')[0]) == int(transID.split('_')[0])) and (int(pkt['payload'][2].split('_')[-1]) < int(transID.split('_')[-1])):
                pktToRemove = pkt
                break
        if pktToRemove:
            # release the cells that were reserved locally for adding
            if pktToRemove['payload'][4] is not None:
                # if it is a llsf response
                # TODO? MAYBE IT IS pktToSend
                # self._llsf_remove_blocked_reservationsRX(self.pktToSend['payload'][2])
                self._llsf_remove_blocked_reservationsRX(pktToRemove['payload'][6])
            else:
                # normal RESPONSE packet
                self._tsch_releaseCells(pktToRemove['payload'][1])
            self.txQueue.remove(pktToRemove)

        # create new packet
        newPacket = {
            'asn':            self.engine.getAsn(),
            'type':           self.SIXTOP_TYPE_RESPONSE,
            'payload':        [self.rank, cells, transID, dir, llsfPayload, numCells, llsfUniqueID], # the payload is the rpl rank
            'retriesLeft':    self.TSCH_MAXTXRETRIES, # do not retransmit broadcast
            'nextTX':         1, # SLOTTED ALOHA: this packet should be sent at the first next possibility
            'destination':    neighbor, # eventual destination of this packet is the neighbor from which the request originated
        }

        # update mote stats
        self._stats_incrementMoteStats('topGeneratedResponse')
        if llsfPayload != None:
            self._stats_incrementMoteStats('topGeneratedResponseLLSF')

        # enqueue packet in TSCH queue
        isEnqueued = self._tsch_enqueue_neighbor(newPacket, neighbor=neighbor)

        if isEnqueued:
            # print 'On mote %s at ASN %s, succesfully enqueued a new 6P RESPONSE message.' % (str(self.id), str(self.engine.asn))
            self._log(
                self.INFO,
                "On mote {0} at ASN {1}, succesfully enqueued a new 6P RESPONSE message.",
                (str(self.id), str(self.engine.asn)),
            )
            self._stats_incrementMoteStats('topSuccessGeneratedResponse')
            if llsfPayload != None:
                self._stats_incrementMoteStats('topSuccessGeneratedResponseLLSF')

            # increment traffic
            if neighbor is self.preferredParent:
                if llsfPayload is not None:
                    self._otf_incrementIncomingTraffic(self)
                else:
                    self._otf_incrementIncomingLLSFTraffic(self)
            else:
                self._otf_incrementIncomingTrafficForChild(neighbor)

        else:
            # print 'On mote %s at ASN %s, UNsuccessfully enqueued a new 6P RESPONSE message.' % (str(self.id), str(self.engine.asn))
            self._log(
                self.INFO,
                "On mote {0} at ASN {1}, UNsuccessfully enqueued a new 6P RESPONSE message.",
                (str(self.id), str(self.engine.asn)),
            )
            # update mote stats
            self._stats_incrementMoteStats('dropped6PFailedEnqueue')
            if llsfPayload != None:
                self._stats_incrementMoteStats('dropped6PFailedEnqueueLLSF')

    def _sixtop_schedule_housekeeping(self):

        self.engine.scheduleIn(
            delay       = self.sixtopHousekeepingPeriod*(0.9+0.2*self.randomGen.random()),
            cb          = self._sixtop_action_housekeeping,
            uniqueTag   = (self.id,'_sixtop_action_housekeeping'),
            priority    = 5,
        )

    def _sixtop_action_housekeeping(self):
        '''
        For each neighbor I have TX cells to, relocate cells if needed.
        '''
        # print '------------------------------------------- COME IN 6P HOUSEKEEPING.'
        #=== tx-triggered housekeeping

        # collect all neighbors I have TX cells to
        txNeighbors = [cell['neighbor'] for (ts,cell) in self.schedule.items() if cell['dir']==self.DIR_TX]

        # remove duplicates
        txNeighbors = list(set(txNeighbors))

        for neighbor in txNeighbors:
            nowCells = self.numCellsToNeighbors.get(neighbor,0)
            assert nowCells == len([t for (t,c) in self.schedule.items() if c['dir']==self.DIR_TX and c['neighbor']==neighbor])

        # do some housekeeping for each neighbor
        for neighbor in txNeighbors:
            self._sixtop_txhousekeeping_per_neighbor(neighbor)

        #=== rx-triggered housekeeping

        # collect neighbors from which I have RX cells that is detected as collision cell
        rxNeighbors = [cell['neighbor'] for (ts,cell) in self.schedule.items() if cell['dir']==self.DIR_RX and cell['rxDetectedCollision']]

        # remove duplicates
        rxNeighbors = list(set(rxNeighbors))

        for neighbor in rxNeighbors:
            nowCells = self.numCellsFromNeighbors.get(neighbor,0)
            assert nowCells == len([t for (t,c) in self.schedule.items() if c['dir']==self.DIR_RX and c['neighbor']==neighbor])

        # do some housekeeping for each neighbor
        for neighbor in rxNeighbors:
            self._sixtop_rxhousekeeping_per_neighbor(neighbor)

        #=== schedule next housekeeping

        self._sixtop_schedule_housekeeping()

    def _sixtop_txhousekeeping_per_neighbor(self,neighbor):
        '''
        For a particular neighbor, decide to relocate cells if needed.
        '''

        #===== step 1. collect statistics

        # pdr for each cell
        cell_pdr = []
        for (ts,cell) in self.schedule.items():
            if cell['neighbor']==neighbor and cell['dir']==self.DIR_TX:
                # this is a TX cell to that neighbor

                # abort if not enough TX to calculate meaningful PDR
                if cell['numTx']<self.NUM_SUFFICIENT_TX:
                    continue

                # calculate pdr for that cell
                recentHistory = cell['history'][-self.NUM_MAX_HISTORY:]
                pdr = float(sum(recentHistory)) / float(len(recentHistory))

                # store result
                cell_pdr += [(ts,pdr)]

        # pdr for the bundle as a whole
        bundleNumTx     = sum([len(cell['history'][-self.NUM_MAX_HISTORY:]) for cell in self.schedule.values() if cell['neighbor']==neighbor and cell['dir']==self.DIR_TX])
        bundleNumTxAck  = sum([sum(cell['history'][-self.NUM_MAX_HISTORY:]) for cell in self.schedule.values() if cell['neighbor']==neighbor and cell['dir']==self.DIR_TX])
        if bundleNumTx<self.NUM_SUFFICIENT_TX:
            bundlePdr   = None
        else:
            bundlePdr   = float(bundleNumTxAck) / float(bundleNumTx)

        #===== step 2. relocate worst cell in bundle, if any
        # this step will identify the cell with the lowest PDR in the bundle.
        # If its PDR is self.sixtopPdrThreshold lower than the average of the bundle
        # this step will move that cell.

        relocation = False

        if cell_pdr:

            # identify the cell with worst pdr, and calculate the average

            worst_ts   = None
            worst_pdr  = None

            for (ts,pdr) in cell_pdr:
                if worst_pdr==None or pdr<worst_pdr:
                    worst_ts  = ts
                    worst_pdr = pdr

            assert worst_ts!=None
            assert worst_pdr!=None

            # ave pdr for other cells
            othersNumTx      = sum([len(cell['history'][-self.NUM_MAX_HISTORY:]) for (ts,cell) in self.schedule.items() if cell['neighbor']==neighbor and cell['dir']==self.DIR_TX and ts != worst_ts])
            othersNumTxAck   = sum([sum(cell['history'][-self.NUM_MAX_HISTORY:]) for (ts,cell) in self.schedule.items() if cell['neighbor']==neighbor and cell['dir']==self.DIR_TX and ts != worst_ts])
            if othersNumTx<self.NUM_SUFFICIENT_TX:
                ave_pdr      = None
            else:
                ave_pdr      = float(othersNumTxAck) / float(othersNumTx)

            # relocate worst cell if "bad enough"
            if ave_pdr and worst_pdr<(ave_pdr/self.sixtopPdrThreshold):

                # log
                self._log(
                    self.INFO,
                    "[6top] relocating cell ts {0} to {1} (pdr={2:.3f} significantly worse than others {3})",
                    (worst_ts,neighbor,worst_pdr,cell_pdr),
                )

                # measure how many cells I have now to that parent
                nowCells = self.numCellsToNeighbors.get(neighbor,0)

                # relocate: add new first
                self._sixtop_cell_reservation_request(neighbor,1)

                # relocate: remove old only when successfully added
                if nowCells < self.numCellsToNeighbors.get(neighbor,0):
                    self._sixtop_cell_deletion_sender(neighbor,[worst_ts])

                    # update stats
                    self._stats_incrementMoteStats('topTxRelocatedCells')

                    # remember I relocated a cell for that bundle
                    relocation = True

        #===== step 3. relocate the complete bundle
        # this step only runs if the previous hasn't, and we were able to
        # calculate a bundle PDR.
        # This step verifies that the average PDR for the complete bundle is
        # expected, given the RSSI to that neighbor. If it's lower, this step
        # will move all cells in the bundle.

        bundleRelocation = False

        if (not relocation) and bundlePdr!=None:

            # calculate the theoretical PDR to that neighbor, using the measured RSSI
            rssi            = self.getRSSI(neighbor)
            theoPDR         = Topology.Topology.rssiToPdr(rssi)

            # relocate complete bundle if measured RSSI is significantly worse than theoretical
            if bundlePdr<(theoPDR/self.sixtopPdrThreshold):
                for (ts,_) in cell_pdr:

                    # log
                    self._log(
                        self.INFO,
                        "[6top] relocating cell ts {0} to {1} (bundle pdr {2} << theoretical pdr {3})",
                        (ts,neighbor,bundlePdr,theoPDR),
                    )

                    # measure how many cells I have now to that parent
                    nowCells = self.numCellsToNeighbors.get(neighbor,0)

                    # relocate: add new first
                    self._sixtop_cell_reservation_request(neighbor,1)

                    # relocate: remove old only when successfully added
                    if nowCells < self.numCellsToNeighbors.get(neighbor,0):

                        self._sixtop_cell_deletion_sender(neighbor,[ts])

                        bundleRelocation = True

                # update stats
                if bundleRelocation:
                    self._stats_incrementMoteStats('topTxRelocatedBundles')

    def _sixtop_rxhousekeeping_per_neighbor(self,neighbor):
        '''
        The RX node triggers a relocation when it has heard a packet
        from a neighbor it did not expect ('rxDetectedCollision')
        '''

        rxCells = [(ts,cell) for (ts,cell) in self.schedule.items() if cell['dir']==self.DIR_RX and cell['rxDetectedCollision'] and cell['neighbor']==neighbor]

        relocation = False
        for ts,cell in rxCells:

            # measure how many cells I have now from that child
            nowCells = self.numCellsFromNeighbors.get(neighbor,0)

            # relocate: add new first
            self._sixtop_cell_reservation_request(neighbor,1,dir=self.DIR_RX)

            # relocate: remove old only when successfully added
            if nowCells < self.numCellsFromNeighbors.get(neighbor,0):
                neighbor._sixtop_cell_deletion_sender(self,[ts])

                # remember I relocated a cell
                relocation = True

        if relocation:
            # update stats
            self._stats_incrementMoteStats('topRxRelocatedCells')

    def lookForLargestGapReceptionTimeslot(self, tsList):
        if len(tsList) == 1:
            return tsList[0]
        listOfTuples = [(x,y) for (x, y) in zip(tsList[1:], tsList[:-1])]
        diff = -1
        largestGapTimeslot = None
        for tp in listOfTuples:
            if tp[0] - tp[1] > diff:
                diff = tp[0] - tp[1]
                largestGapTimeslot = tp[0]
        return largestGapTimeslot

    def getAvailableTimeslot(self, firstCellToTry, availableTimeslots, alreadyReservedCells = None):
        listOfTsOfAlreadyReservedCells = []
        if alreadyReservedCells != None:
            listOfTsOfAlreadyReservedCells = [ts for (ts, ch) in alreadyReservedCells]
        availableTimeslots = sorted(list(set(availableTimeslots) - set(listOfTsOfAlreadyReservedCells)))
        timeslotCandidatesGood = sorted(slot for slot in availableTimeslots if slot >= firstCellToTry)
        if len(timeslotCandidatesGood) > 0:
            return timeslotCandidatesGood[0]
        else:
            return None

    def _sixtop_cell_get_all_recurrent_timeslots(self):
        '''
            Return all timeslots that are occupied by the recurrent reservation during a predefined buffer period
            These are the timeslots that will be occupied in A slotframe between this ASN and the end of a predefined buffer period
        '''
        with self.dataLock:
            timeslots = []
            bufferEndASN = self.engine.asn + self.bufferPeriod

            for resTuple in self.LLSFReservationsRXTuples:
                startASN = resTuple[3]
                endASN = resTuple[4]
                interval = resTuple[2]
                if startASN > bufferEndASN: # skip this tuple
                    continue # go to next tuple
                reservationASNs = self._llsf_get_reservationASNs(startASN, endASN, interval)
                for resASN in reservationASNs:
                    if resASN >= self.engine.asn and resASN <= bufferEndASN: # falls in the buffer, store it
                        ts = resASN % self.settings.slotframeLength
                        timeslots.append(ts)
            for resTuple in self.LLSFReservationsTXTuples:
                startASN = resTuple[3]
                endASN = resTuple[4]
                interval = resTuple[2]
                if startASN > bufferEndASN: # skip this tuple
                    continue
                reservationASNs = self._llsf_get_reservationASNs(startASN, endASN, interval)
                for resASN in reservationASNs:
                    if resASN >= self.engine.asn and resASN <= bufferEndASN: # falls in the buffer, store it
                        ts = resASN % self.settings.slotframeLength
                        timeslots.append(ts)
            for resTuple in self.blockedLLSFReservationsRXTuples:
                startASN = resTuple[3]
                endASN = resTuple[4]
                interval = resTuple[2]
                if startASN > bufferEndASN: # skip this tuple
                    continue
                reservationASNs = self._llsf_get_reservationASNs(startASN, endASN, interval)
                for resASN in reservationASNs:
                    if resASN >= self.engine.asn and resASN <= bufferEndASN: # falls in the buffer, store it
                        ts = resASN % self.settings.slotframeLength
                        timeslots.append(ts)
            for resTuple in self.blockedLLSFReservationsTXTuples:
                startASN = resTuple[3]
                endASN = resTuple[4]
                interval = resTuple[2]
                if startASN > bufferEndASN: # skip this tuple
                    continue
                reservationASNs = self._llsf_get_reservationASNs(startASN, endASN, interval)
                for resASN in reservationASNs:
                    if resASN >= self.engine.asn and resASN <= bufferEndASN: # falls in the buffer, store it
                        ts = resASN % self.settings.slotframeLength
                        timeslots.append(ts)

            # make it unique
            timeslots = set(timeslots)

            return timeslots

    def _sixtop_cell_reservation_request(self, neighbor, numCells, dir=DIR_TX):
        ''' tries to reserve numCells cells to a neighbor. '''

        with self.dataLock:
            if numCells > 0:
                # Check for ongoing transactions
                if neighbor in self.sixtopTransactionTX and self.sixtopTransactionTX[neighbor][3] > 0:
                    return False

                # CHECK WHICH CELLS ARE FREE TO RESERVE WITH THE RECEIVER

                # this is coded like this to enable the frame switching
                selfSchedule = self.schedule
                neighborSchedule = neighbor.schedule
                # make sure that 6P reservations only happen in the normal frames
                if self.settings.switchFrameLengths:
                    selfSchedule = self.defaultSchedule
                if self.settings.switchFrameLengths:
                    neighborSchedule = neighbor.defaultSchedule

                cells                 = None
                cellsListNoDir        = None
                cellList              = None

                # make sure the cells at the end add up
                # what happens if a timeslot is already taken
                if self.settings.sf == 'chang' or self.settings.sf == 'recurrent_chang':
                    numCellsExtra = numCells + self.NUMBER_BACKUP_RESERVATIONS_NORMAL # backup cells, send them with the request to make sure you have some free cells
                    # the current available cells
                    blockedCellListTSs = [ts for (ts, ch) in self.blockedCellList]
                    # these are the timeslots that will be occupied in A slotframe between this ASN and the end of a predefined buffer period
                    recurrentReservationTimeslots = self._sixtop_cell_get_all_recurrent_timeslots()
                    availableTimeslots = sorted(list(set(range(self.settings.slotframeLength))-set(selfSchedule.keys())-set(blockedCellListTSs)-recurrentReservationTimeslots))
                    # the current timeslots you have of RX neighbors
                    rxNeighborsTimeslots = OrderedDict()
                    # the timeslot after the largest gap of reception timeslots for RX neighbors
                    rxNeighborsLargestGapTimeslot = OrderedDict()

                    # 1) look for all reception timeslots
                    if neighbor == self.preferredParent:
                        for ts, cell in self.schedule.iteritems():
                            if cell['dir'] == self.DIR_RX and not cell['llsfCell']:
                                if cell['neighbor'] in rxNeighborsTimeslots:
                                    rxNeighborsTimeslots[cell['neighbor']].append(ts)
                                else:
                                    rxNeighborsTimeslots[cell['neighbor']] = [ts]
                    else:
                        # if you reserve to child, only look for RXs to that child so you can communicate fast with that child
                        for ts, cell in self.schedule.iteritems():
                            # it has to be RX cells, but not the neighbor (= parent) to which you want to send
                            if cell['dir'] == self.DIR_RX and cell['neighbor'] == neighbor and not cell['llsfCell']:
                                if cell['neighbor'] in rxNeighborsTimeslots:
                                    rxNeighborsTimeslots[cell['neighbor']].append(ts)
                                else:
                                    rxNeighborsTimeslots[cell['neighbor']] = [ts]

                    # 2) look for largest gap
                    for rxNeighbor, tsList in rxNeighborsTimeslots.iteritems():
                        rxNeighborsLargestGapTimeslot[rxNeighbor] = self.lookForLargestGapReceptionTimeslot(sorted(tsList))

                    # 3) distribute the cells in a random manner
                    rxNbs = rxNeighborsTimeslots.keys()
                    handledrxNbs = []
                    cellsToHandout = numCellsExtra
                    reservationTimeslots = OrderedDict()
                    # keep doing this until all cells are handed out
                    # AND only do this when there are receiving neighbors
                    while cellsToHandout > 0 and len(rxNeighborsTimeslots.keys()) > 0:
                        # reset if we went through the entire list of neighbors
                        if len(rxNbs) == 0:
                            rxNbs = [rxNb for rxNb in handledrxNbs]
                            handledrxNbs = []
                        # find random neighbor
                        index = self.randomGen.randint(0, len(rxNbs)-1)
                        if rxNbs[index] not in reservationTimeslots:
                            reservationTimeslots[rxNbs[index]] = 0
                        reservationTimeslots[rxNbs[index]] += 1 # give this neighbor a cell
                        # move neighbor
                        handledrxNbs.append(rxNbs[index])
                        rxNbs.remove(rxNbs[index])
                        # decrement the number of cells to hand out
                        cellsToHandout -= 1

                    # 4) now finally search for the actual cells for each neighbor
                    for rxNeighbor, timeslotsToReserve in reservationTimeslots.iteritems():
                        nextToGapTimeslot = rxNeighborsLargestGapTimeslot[rxNeighbor] + 1 # + 1, b/c the cell next to the gap
                        channel = self.randomGen.randint(0,self.settings.numChans-1)
                        while timeslotsToReserve > 0:
                            # find an available timeslot
                            availableTimeslot = self.getAvailableTimeslot(nextToGapTimeslot, availableTimeslots, cellsListNoDir)
                            # if None, no availability was found
                            if availableTimeslot:
                                if cellsListNoDir == None:
                                    cellsListNoDir = []
                                cellsListNoDir.append((availableTimeslot, channel))
                            # decrement the number of cells to reserve for this rx neighbor
                            timeslotsToReserve -= 1

                    # 5) if no or not enough cells got reserved by LLSF's way, reserve them here randomly
                    if cellsListNoDir is None or len(cellsListNoDir) < numCellsExtra:
                        toReserveRandomly = numCellsExtra
                        if cellsListNoDir is not None: # if we did manage to reserve some cells already
                            toReserveRandomly = numCellsExtra - len(cellsListNoDir)
                        while toReserveRandomly > 0:
                            allTSs = range(0, self.settings.slotframeLength)
                            self.randomGen.shuffle(allTSs)
                            # availableTimeslot = self.getAvailableTimeslot(random.randint(0, self.settings.slotframeLength-1), availableTimeslots, None)
                            availableTimeslot = None
                            while availableTimeslot is None and len(allTSs) > 0:
                                # you have to pass cellListNoDir, because in a second iteration of the available cells could be reserved in the previous iteration
                                randomTS = allTSs.pop()
                                availableTimeslot = self.getAvailableTimeslot(randomTS, availableTimeslots, cellsListNoDir)
                                channel = self.randomGen.randint(0,self.settings.numChans-1)
                                if availableTimeslot is not None:
                                    if cellsListNoDir == None:
                                        cellsListNoDir = []
                                    cellsListNoDir.append((availableTimeslot, channel))
                            toReserveRandomly -= 1 # we tried one more randomly

                else:
                    numCellsExtra = numCells + self.NUMBER_BACKUP_RESERVATIONS_NORMAL # backup cells, send them with the request to make sure you have some free cells
                    # the blocked timeslots that can not be used in other operations
                    blockedCellListTSs = [ts for (ts, ch) in self.blockedCellList]
                    # TODO: send the schedule in the 6P message? Maybe in a later version.
                    # gdaneels, NOTE: this can stay slotframelength because 6P only does reservations in normal slotframes
                    availableTimeslots    = list(set(range(self.settings.slotframeLength))-set(selfSchedule.keys())-set(blockedCellListTSs))
                    availableTimeslots    = sorted(availableTimeslots)
                    # availableTimeslots    = list(set(range(self.settings.slotframeLength))-set(neighborSchedule.keys())-set(selfSchedule.keys())-set(blockedCellListTSs))
                    self.randomGen.shuffle(availableTimeslots)
                    cells                 = OrderedDict([(ts,self.randomGen.randint(0,self.settings.numChans-1)) for ts in availableTimeslots[:numCellsExtra]])
                    cellsListNoDir        = [(ts, ch) for ts, ch in cells.iteritems()]

                # if all cells were occupied, you should not send a add request
                if cellsListNoDir != None and len(cellsListNoDir) > 0:
                    # block these cells so in other sixtop operations, no one can use these cells
                    self._tsch_blockCells(cellsListNoDir)

                    # enqueue a packet for the sixtop reservation
                    # numCells is the actual number of cells you have to reserve (first cell is the actual number you want to reserve, second one is important for LLSF reservations)
                    transID = self._sixtop_action_enqueue_ADD(cellsListNoDir, dir, dest=neighbor, llsfPayload = None, numCells=(numCells, None))

                    if transID:
                        # remember the transaction ID and the cells you wanted, the direction and that the transaction is ongoing
                        self.sixtopTransactionTX[neighbor] = (transID, cellsListNoDir, dir, self.sixtop_timout_init, False)
                else:
                    self._log(
                        self.INFO,
                        '[6top] On mote {0}, found no available timeslot for normal reservation.',
                        (str(self.id)),
                    )

    def _sixtop_cell_llsf_reservation_request(self, neighbor, wantedTSTuple, dir=DIR_TX, llsfUniqueID=None, numCellsOrig=1):
        ''' tries to reserve numCells cells to a neighbor in a LLSF manner '''

        with self.dataLock:

            if wantedTSTuple is not None:
                # Check for ongoing transactions

                if neighbor in self.sixtopTransactionTX and self.sixtopTransactionTX[neighbor][3] > 0:
                    return False

                # this is coded like this to enable the frame switching
                selfSchedule = self.schedule
                # make sure that 6P reservations only happen in the normal frames
                if self.settings.switchFrameLengths:
                    selfSchedule = self.defaultSchedule

                # the blocked timeslots that can not be used in other operations
                blockedCellListTSs = [ts for (ts, ch) in self.blockedCellList]
                # TODO: maybe this should also hold future collisions
                # gdaneels, NOTE: this can stay slotframelength because 6P only does reservations in normal slotframes
                availableTimeslots    = list(set(range(self.settings.slotframeLength))-set(selfSchedule.keys())-set(blockedCellListTSs))
                # sort them
                availableTimeslots    = sorted(availableTimeslots)
                # availableTimeslots    = list(set(range(self.settings.slotframeLength))-set(neighborSchedule.keys())-set(selfSchedule.keys())-set(blockedCellListTSs))
                llsfCellList = []
                # for wantedTSTuple in wantedTSTuples:
                timeslot = wantedTSTuple[0]
                interval = wantedTSTuple[1]
                startASN = wantedTSTuple[2]
                endASN = wantedTSTuple[3]

                cellsListNoDir = []
                if False:
                # look for a slot
                    timeslotCandidates = sorted(slot for slot in availableTimeslots if slot >= timeslot)
                    if timeslotCandidates != []: # if a slot is found, otherwise skip
                        ts = timeslotCandidates[0]
                        ch = self.randomGen.randint(0,self.settings.numChans-1)
                        cell = (ts, ch)
                        cellsListNoDir.append(cell)
                        llsfcell = (ts,ch,interval,startASN,endASN)
                        llsfCellList += [llsfcell]
                        self.blockedLLSFReservationsTX += llsfcell
                else: # take a random cell
                    self.randomGen.shuffle(availableTimeslots)
                    ch = self.randomGen.randint(0,self.settings.numChans-1)
                    ts = 0 # this does not matter and is only here to be compatible with the rest of the code
                    solver = CollisionSolver.CollisionSolver()

                    # include the ETX, add extra cells if necessary
                    etx = self._estimateETX(neighbor)
                    if etx>self.RPL_MAX_ETX: # cap ETX
                        etx  = self.RPL_MAX_ETX

                    numCellsToReserve = int(math.ceil(numCellsOrig*etx)) # == etx
                    if numCellsToReserve > self.MAX_RESF_RESERVATIONS:
                        numCellsToReserve = self.MAX_RESF_RESERVATIONS
                    numCellsTuple = (numCellsToReserve, 1) # first element is the actual number to reserve, the second one is the original number of cells

                    thresholds = []

                    # keep looking for 1 (the actual reservation) and NUMBER_BACKUP_RESERVATIONS backup reservations
                    # only one will be used

                    asnToCollisionRate = []

                    bufferInterval = self.MAX_RESF_RESERVATIONS + self.settings.reservationBuffer
                    # 1) get collisions for X first cells.
                    for startASNIndex in range(startASN, (startASN + bufferInterval)):
                        collisions = []

                        # 1.1) get the collision with the already enabled/blocked reservations.
                        for resTuple in self.LLSFReservationsRXTuples:
                            collisions += solver.getCollisions(startASNIndex, endASN, interval, resTuple[3], resTuple[4], resTuple[2])
                        for resTuple in self.LLSFReservationsTXTuples:
                            collisions += solver.getCollisions(startASNIndex, endASN, interval, resTuple[3], resTuple[4], resTuple[2])
                        for resTuple in self.blockedLLSFReservationsRXTuples:
                            collisions += solver.getCollisions(startASNIndex, endASN, interval, resTuple[3], resTuple[4], resTuple[2])
                        for resTuple in self.blockedLLSFReservationsTXTuples:
                            collisions += solver.getCollisions(startASNIndex, endASN, interval, resTuple[3], resTuple[4], resTuple[2])

                        # 1.2) get the collisions with the shared reservations
                        sharedTSs = [ts for (ts, ch_shared, nMote) in self.getSharedCells()]
                        sharedCollisions = []
                        for sharedTS in sharedTSs:
                            sharedCollisions += solver.getCollisions(startASNIndex, endASN, interval, sharedTS, endASN, self.settings.slotframeLength) # define the shared cells also as tuples.
                            collisions += sharedCollisions

                        uniqueCollisions = set(collisions)
                        lenUniqueCollisions = len(uniqueCollisions)

                        numReservationASNs = self._llsf_get_numReservationASNs(startASNIndex, endASN, interval)
                        collisionRate = lenUniqueCollisions / float(numReservationASNs)
                        collisionRate = float(format(collisionRate, '.2f')) # rounded to two decimals

                        if collisionRate not in self.engine.collisionPercentages:
                            self.engine.collisionPercentages.append(collisionRate)

                        asnToCollisionRate += [(startASNIndex, collisionRate, lenUniqueCollisions)]

                        # print 'len / num: %s / %s' % (lenUniqueCollisions, numReservationASNs)
                        # print 'Shared Collisions: %s' % str(sharedCollisions)
                        # print 'ASN TO CollisionRate: %s' % str(asnToCollisionRate)

                    assert len(asnToCollisionRate) > 0

                    sortedASNToCollisionRate = sorted(asnToCollisionRate, key=lambda x: (x[1],x[0]))
                    # print 'SORTED ASN TO CollisionRate: %s' % str(sortedASNToCollisionRate)

                    # 2) add the cells to the reservation list
                    for index in range(0, self.MAX_RESF_RESERVATIONS):
                        ch = self.randomGen.randint(0, self.settings.numChans-1) # channel
                        startASNIndex = sortedASNToCollisionRate[index][0] # get start ASN
                        llsfCellList += [(ts, ch, interval, startASNIndex, endASN)] # add to reservation list
                        if sortedASNToCollisionRate[index][1] not in self.engine.collisionPercentages:
                            self.engine.collisionPercentages.append(sortedASNToCollisionRate[index][1]) # add the collision percentage
                        self.engine.scheduledPossibleTXRecurrentCollisions += sortedASNToCollisionRate[index][2] # add the absolute number of scheduled collisions
                        self._log(
                            self.INFO,
                            '[6top] On mote {0}, adding this tuple to the reservation list: ({1}, {2}, {3}). Having soo many collisions with other recurrent reservations: {4}.',
                            (str(self.id), startASNIndex, endASN, interval, sortedASNToCollisionRate[index][2]),
                        )
                    # print llsfCellList
                    # while len(llsfCellList) < numCellsToReserveWithBackup:
                    #     ch = self.randomGen.randint(0, self.settings.numChans-1)
                    #     first = True
                    #     # print startASN
                    #     if startASN >= (endASN - interval):
                    #         break
                    #
                    #     # this are the values you start each slot search with
                    #     originalStartASN = startASN
                    #     COLLISION_THRESHOLD = self.settings.collisionRateThreshold
                    #
                    #     reservationASNs = self._llsf_get_reservationASNs(startASN, endASN, interval)
                    #     if len(reservationASNs) <= 3 and COLLISION_THRESHOLD < 0.5:
                    #         COLLISION_THRESHOLD = 0.5
                    #     uniqueCollisions = None
                    #     lenUniqueCollisions = None
                    #     while (first or (lenUniqueCollisions / float(len(reservationASNs))) > COLLISION_THRESHOLD) and (1.0 - COLLISION_THRESHOLD) > 0.00001:
                    #
                    #         collisions = []
                    #
                    #         # TODO:  for updates I should find another way of not having collisions.
                    #         # if I include the llsfUniqueID != resTuple[7], it prohibits nodes finding reservations for example
                    #         # node x wants to send a reservation to its parent (to be put in LLSFReservationsTXTuples), but the reservation formula should not match with the reservation coming from his child (LLSFReservationsRXTuples)
                    #         # this reservations will have the same ID, so the collisions are not calculated
                    #         for resTuple in self.LLSFReservationsRXTuples:
                    #             # if llsfUniqueID != resTuple[7]: # if there is already a reservation with this ID, the new one is probably an update
                    #             cols = solver.getCollisions(startASN, endASN, interval, resTuple[3], resTuple[4], resTuple[2])
                    #             collisions += cols
                    #             # print 'comparing (%d,%d,%d) and (%d,%d,%d)' % (startASN, endASN, interval, resTuple[3], resTuple[4], resTuple[2])
                    #             # print 'RX enabled: %s' % str(cols)
                    #             # if startASN == resTuple[3] and endASN == resTuple[4] and interval == resTuple[2]:
                    #             #     print 'unique ID %s' % str(llsfUniqueID)
                    #             #     print str(resTuple)
                    #                 # raise -1
                    #         for resTuple in self.LLSFReservationsTXTuples:
                    #             # if llsfUniqueID != resTuple[7]: # if there is already a reservation with this ID, the new one is probably an update
                    #             cols = solver.getCollisions(startASN, endASN, interval, resTuple[3], resTuple[4], resTuple[2])
                    #             collisions += cols
                    #             # print 'comparing (%d,%d,%d) and (%d,%d,%d)' % (startASN, endASN, interval, resTuple[3], resTuple[4], resTuple[2])
                    #             # print 'TX enabled: %s' % str(cols)
                    #             # if startASN == resTuple[3] and endASN == resTuple[4] and interval == resTuple[2]:
                    #             #     print 'unique ID %s' % str(llsfUniqueID)
                    #             #     print str(resTuple)
                    #                 # raise -1
                    #         for resTuple in self.blockedLLSFReservationsRXTuples:
                    #             # if llsfUniqueID != resTuple[7]: # if there is already a reservation with this ID, the new one is probably an update
                    #             cols = solver.getCollisions(startASN, endASN, interval, resTuple[3], resTuple[4], resTuple[2])
                    #             collisions += cols
                    #             # print 'comparing (%d,%d,%d) and (%d,%d,%d)' % (startASN, endASN, interval, resTuple[3], resTuple[4], resTuple[2])
                    #             # print 'RX blocked: %s' % str(cols)
                    #             # if startASN == resTuple[3] and endASN == resTuple[4] and interval == resTuple[2]:
                    #             #     print 'unique ID %s' % str(llsfUniqueID)
                    #             #     print str(resTuple)
                    #                 # raise -1
                    #         for resTuple in self.blockedLLSFReservationsTXTuples:
                    #             # if llsfUniqueID != resTuple[7]: # if there is already a reservation with this ID, the new one is probably an update
                    #             cols = solver.getCollisions(startASN, endASN, interval, resTuple[3], resTuple[4], resTuple[2])
                    #             collisions += cols
                    #             # print 'comparing (%d,%d,%d) and (%d,%d,%d)' % (startASN, endASN, interval, resTuple[3], resTuple[4], resTuple[2])
                    #             # print 'TX blocked: %s' % str(cols)
                    #             # if startASN == resTuple[3] and endASN == resTuple[4] and interval == resTuple[2]:
                    #             #     print 'unique ID %s' % str(llsfUniqueID)
                    #             #     print str(resTuple)
                    #                 # raise -1
                    #         # make the collisions unique (normally they should be unique)
                    #         # print 'collisions: %s' % str(collisions)
                    #         uniqueCollisions = set(collisions)
                    #         # print 'unique collisions: %s' % str(uniqueCollisions)
                    #         lenUniqueCollisions = len(uniqueCollisions)
                    #
                    #         for col in uniqueCollisions:
                    #             if col < 0:
                    #                 raise -1
                    #
                    #         # only look at a buffer period, because otherwise it will be very difficult to find a cell
                    #         # bufferEndASN = self.engine.asn + self.bufferPeriod
                    #         t = (startASN, endASN, interval)
                    #         # print 'On mote %d, sending a recurrent reservation for tuple: %s. Having soo many collisions with other recurrent reservations: %d.' % (self.id, str(t), lenUniqueCollisions)
                    #         self._log(
                    #             self.INFO,
                    #             '[6top] On mote {0}, sending a recurrent reservation for tuple: {1}. Having soo many collisions with other recurrent reservations: {2}.',
                    #             (str(self.id), str(t), lenUniqueCollisions),
                    #         )
                    #         reservationASNsToTSs = [resASN % self.settings.slotframeLength for resASN in reservationASNs]
                    #         # print 'On mote %d, the recurrent reservations to timeslots: %s.' % (self.id, reservationASNsToTSs)
                    #         self._log(
                    #             self.INFO,
                    #             '[6top] On mote {0}, the recurrent reservations to timeslots: {1}.',
                    #             (str(self.id), str(reservationASNsToTSs)),
                    #         )
                    #
                    #         sharedTSs = [ts for (ts, ch_shared, nMote) in self.getSharedCells()]
                    #         for resASN in reservationASNs: # also check how many collide with SHARED slots
                    #             # if resASN <= bufferEndASN:
                    #             ts = resASN % self.settings.slotframeLength
                    #             if ts in sharedTSs: # if this timeslot is in the ts, increment the number of collisions
                    #                 lenUniqueCollisions += 1
                    #                 # print 'On mote %d, having a collision with a shared cell.' % self.id
                    #                 self._log(
                    #                     self.INFO,
                    #                     '[6top] On mote {0}, having a collision with a shared cell.',
                    #                     (str(self.id)),
                    #                 )
                    #
                    #         colrate = lenUniqueCollisions / float(len(reservationASNs))
                    #
                    #         if colrate not in self.engine.collisionPercentages:
                    #             self.engine.collisionPercentages.append(colrate)
                    #
                    #         self._log(
                    #             self.INFO,
                    #             '[6top] On mote {0}, collision rate is: {1} (in request)',
                    #             (str(self.id), colrate),
                    #         )
                    #
                    #         if first:
                    #             first = False
                    #         if lenUniqueCollisions / float(len(reservationASNs)) > COLLISION_THRESHOLD:
                    #             startASN += 1
                    #             if startASN >= (endASN - interval):
                    #                 break
                    #             elif startASN >= originalStartASN + int(self.settings.slotframeLength/2):
                    #                 # if we tried more than int(self.settings.slotframeLength/2) slots with the current collision rate threshold, increase the threshold
                    #                 COLLISION_THRESHOLD += self.settings.collisionRateThreshold
                    #                 startASN = originalStartASN
                    #                 self._log(
                    #                     self.INFO,
                    #                     '[6top] On mote {0}, SETTING COLLISION THRESHOLD TO: {1}  and resetting startASN to {2} (in request)',
                    #                     (str(self.id), COLLISION_THRESHOLD, startASN),
                    #                 )
                    #                 # raise -1
                    #             reservationASNs = self._llsf_get_reservationASNs(startASN, endASN, interval)
                    #             continue
                    #
                    #         self.engine.scheduledPossibleTXRecurrentCollisions += lenUniqueCollisions
                    #
                    #     cellsListNoDir.append((ts, ch))
                    #     llsfCellList += [(ts,ch,interval,startASN,endASN)]
                    #     thresholds.append(COLLISION_THRESHOLD)
                    #
                    #     # for possible next iteration
                    #     startASN += 1
                    #     reservationASNs = self._llsf_get_reservationASNs(startASN, endASN, interval)

                # print 'On mote %d, enqueuing a new ADD for LLSF reservation with ID %s.' % (self.id, llsfUniqueID)
                self._log(
                    self.INFO,
                    '[6top] On mote {0}, enqueuing a new ADD for LLSF reservation with ID {1}.',
                    (self.id, llsfUniqueID),
                )

                if len(llsfCellList) > 0: # if there were successfull request reservations found
                    # enqueue a packet for the sixtop reservation
                    transID = self._sixtop_action_enqueue_ADD(cellsListNoDir, dir, dest=neighbor, llsfPayload = llsfCellList, numCells = numCellsTuple, llsfUniqueID=llsfUniqueID, thresholds=thresholds)

                    if transID:
                        # remember the transaction ID and the cells you wanted, the direction and that the transaction is ongoing
                        self.sixtopTransactionTX[neighbor] = (transID, cellsListNoDir, dir, self.sixtop_timout_init, True, wantedTSTuple, llsfUniqueID, numCellsOrig)

                    return transID
                else:
                    return self.RESERVATION_NOTFEASABLE # if a -1 is returned, this reservation should be cancelled

    # remove recurrent cells at the end of a cycle
    def _sixtop_remove_llsf_reservations(self):
        ''' remove recurrent cells at the end of a cycle '''
        toRemoveTSs = []
        for (ts,cell) in self.schedule.iteritems():
            if cell['llsfCell']:
                toRemoveTSs.append(ts)
                if cell['dir'] == self.DIR_TX:
                    self.numLLSFCellsToNeighbors[cell['neighbor']] -= 1
                elif cell['dir'] == self.DIR_RX:
                    self.numLLSFCellsFromNeighbors[cell['neighbor']] -= 1

        # at the end of a cycle this should be zero
        for nb, numCells in self.numLLSFCellsFromNeighbors.iteritems():
            assert numCells == 0
        for nb, numCells in self.numLLSFCellsToNeighbors.iteritems():
            assert numCells == 0

        for ts in toRemoveTSs:
            del self.schedule[ts]

    def _sixtop_enable_llsf_reservations(self, startASN, endASN):
        # enable the RX reservations
        for reservationRXTuple in self.LLSFReservationsRXTuples:
            reservationASNs = self._llsf_get_reservationASNs(reservationRXTuple[3], reservationRXTuple[4], reservationRXTuple[2])
            for resASN in reservationASNs:
                if resASN >= startASN and resASN <= endASN:
                    ch = reservationRXTuple[1]
                    neighbor = reservationRXTuple[6]
                    ts = resASN % self.settings.slotframeLength

                    # this is coded like this to enable the frame switching
                    selfSchedule = self.schedule
                    # make sure that 6P reservations only happen in the normal frames
                    if self.settings.switchFrameLengths:
                        selfSchedule = self.defaultSchedule

                    # only if there is no other already scheduled cell in the schedule (a normal reservation), allwo this to be scheduled
                    if ts not in selfSchedule.keys():
                        cellList = []
                        # log
                        self._log(
                            self.INFO,
                            '[6top] add RX cell ts={0} (ASN {1}), ch={2} on mote {3}.',
                            (ts, resASN, ch, self.id),
                        )
                        cellList         += [(ts,ch,self.DIR_RX)]
                        self._tsch_addCells(neighbor, cellList, selfSchedule, llsfCells=True)

                        # update counters
                        if neighbor not in self.numLLSFCellsFromNeighbors:
                            self.numLLSFCellsFromNeighbors[neighbor]   = 0
                        self.numLLSFCellsFromNeighbors[neighbor]      += 1

        for reservationTXTuple in self.LLSFReservationsTXTuples:
            reservationASNs = self._llsf_get_reservationASNs(reservationTXTuple[3], reservationTXTuple[4], reservationTXTuple[2])
            for resASN in reservationASNs:
                if resASN >= startASN and resASN <= endASN:
                    ch = reservationTXTuple[1]
                    neighbor = reservationTXTuple[6]
                    ts = resASN % self.settings.slotframeLength

                    # this is coded like this to enable the frame switching
                    selfSchedule = self.schedule
                    # make sure that 6P reservations only happen in the normal frames
                    if self.settings.switchFrameLengths:
                        selfSchedule = self.defaultSchedule

                    # only if there is no other already scheduled cell in the schedule (a normal reservation), allwo this to be scheduled
                    if ts not in selfSchedule.keys():
                        cellList = []
                        # log
                        self._log(
                            self.INFO,
                            '[6top] add TX cell ts={0} (ASN {1}), ch={2} on mote {3}.',
                            (ts, resASN, ch, self.id),
                        )
                        cellList         += [(ts,ch,self.DIR_TX)]
                        self._tsch_addCells(neighbor, cellList, selfSchedule, llsfCells=True)

                        # update counters
                        if neighbor not in self.numLLSFCellsToNeighbors:
                            self.numLLSFCellsToNeighbors[neighbor]     = 0
                        self.numLLSFCellsToNeighbors[neighbor]        += 1

    def _sixtop_action_receive_RESPONSE(self,type,smac,payload):
        ''' tries to reserve numCells cells to a neighbor. '''

        with self.dataLock:

            # get the necessary information
            neighbor = smac
            cells       = payload[1] # important: this list of cells will be less than the list blocked when sending the add (extra cells were sent as backup reservations)
            transID     = payload[2]
            llsfPayload = payload[4]
            numCells    = payload[5][0] # the number of cells incl the etx cells
            numCellsOrig = payload[5][1] # the orig number of cells you want to reserve
            llsfUniqueID = payload[6]

            # or the last transaction expired and there is no new one
            # or the last transaction expired and there is already a new one
            # = this is probably a RESPONSE to old ADD, discard it
            if smac not in self.sixtopTransactionTX or transID != self.sixtopTransactionTX[smac][0]:
                return False # the message can not be ack'd

            # TODO: this should always be true?
            assert transID == self.sixtopTransactionTX[smac][0]

            # second element in tuple is the request directio
            # the dir that is in the RESPONSE packet, is the direction in the view of the receiver, so we should take the original
            dir = self.sixtopTransactionTX[smac][2]

            cellList    = []
            # for (ts,ch) in cells.iteritems():
            for (ts,ch) in cells:
                # log
                self._log(
                    self.INFO,
                    '[6top] add TX cell ts={0},ch={1} from {2} to {3}',
                    (ts,ch,self.id,neighbor.id),
                )
                cellList += [(ts,ch,dir)]

            # enable the blocked LLSF reservations
            if llsfPayload is not None:
                # print 'On mote %d, the llsfPayload: %s' % (self.id, llsfPayload)

                # remove outdated reservations
                # if I receive a RESPONSE from a parent, we can safely remove old reservations with an older llsfUniqueID
                # it is also at this point where you can start removing the old uniqueIDs
                if '_c_' in llsfUniqueID: # this means this is an update
                    prefix = llsfUniqueID.split('_c_')[0]
                    IDList = self._sixtop_remove_llsf_reservation_with_prefix(prefix, llsfUniqueID, dir='TX')
                    uniqueIDList = list(set(IDList))
                    for idx in uniqueIDList: # remove the ID(s) in the list
                        # print 'removing %s' % idx
                        del self.llsfTransIDs[idx]

                # block the recurrent reservation cells
                self._llsf_enable_blocked_reservationsTX(llsfUniqueID, llsfPayload) # based on llsfUniqueID
                if llsfPayload == []: # the motes did not agree upon any reservation, make a new one
                    raise -1 # NOTE: with the current collision implementation, this should not happen anymore
                    # get the old reservation tuple, update the formula (startASN)
                    timeslot = self.sixtopTransactionTX[smac][5][0]
                    interval = self.sixtopTransactionTX[smac][5][1]
                    startASN = self.sixtopTransactionTX[smac][5][2]
                    endASN = self.sixtopTransactionTX[smac][5][3]
                    numCellsOrig = self.sixtopTransactionTX[smac][7] # original number of cells
                    oldReservationTuple = (timeslot, interval, startASN, endASN)
                    newReservationTuple = (timeslot, interval, startASN + self.NUMBER_BACKUP_RESERVATIONS + 1, endASN)
                    self._log(
                        self.ERROR,
                        '[6top] did not manage to agree on a reservation with tuple {0}, starting a new one with reservation tuple {1}',
                        (oldReservationTuple, newReservationTuple),
                    )
                    # here we should send a new reservation
                    # NOTE: here we can not use self._sixtop_resend_LLSF_reservation_immediately because that function also deletes the transID and this already happens in activeCell after returning the ACK
                    self._sixtop_resend_LLSF_reservation(newReservationTuple, self.preferredParent, llsfUniqueID, numCellsOrig, transID)
            else:
                # First unblock these cells somewhere!
                # important that you release the cells that are in the transaction because otherwise you maybe release too little cells
                # if you take the cells from the payload
                # The advantage of releasing them like this is that ALL cells are released: also the backup cells that were not reserved
                self._tsch_releaseCells(self.sixtopTransactionTX[smac][1])
                if self.settings.switchFrameLengths: # make sure you add them to the correct schedule
                    self._tsch_addCells(neighbor,cellList, self.defaultSchedule)
                else:
                    self._tsch_addCells(neighbor,cellList)

                # update counters
                if dir==self.DIR_TX:
                    if neighbor not in self.numCellsToNeighbors:
                        self.numCellsToNeighbors[neighbor]     = 0
                    self.numCellsToNeighbors[neighbor]        += len(cells)
                else:
                    if neighbor not in self.numCellsFromNeighbors:
                        self.numCellsFromNeighbors[neighbor]   = 0
                    self.numCellsFromNeighbors[neighbor]      += len(cells)

            if len(cells) != numCells:
                # log
                self._log(
                    self.ERROR,
                    '[6top] scheduled {0} cells out of {1} required between motes {2} and {3}',
                    (len(cells),self.sixtopTransactionTX[smac][1],self.id,neighbor.id),
                )

            return True # the message can be ack'd

    def _llsf_get_reservationASNs(self, startASN, endASN, interval):
        if startASN > endASN:
            print startASN
            print endASN
        assert startASN <= endASN
        reservationASNs = []
        first = True
        count = 0
        while first or reservation < endASN:
            reservation = startASN + count*interval
            if reservation <= endASN:
                reservationASNs.append(reservation)
            if first:
                first = False
            count += 1
        return reservationASNs

    def _llsf_get_numReservationASNs(self, startASN, endASN, interval):
        assert startASN <= endASN
        return (((endASN - startASN) / interval) + 1)

    def _llsf_remove_reservationsRX(self, llsfUniqueID, reservationTuples):
        ''' Remove RX LLSF reservations that were enabled.'''
        toRemoveReservations = []
        for reservationTuple in self.LLSFReservationsRXTuples:
            if llsfUniqueID == reservationTuple[7]:
                toRemoveReservations.append(reservationTuple)

        self._log(
            self.INFO,
            "[sixtop] On mote {0}, removed from LLSFReservationsRX: {1}",
            (self.id, str(toRemoveReservations)),
        )
        for toRemove in toRemoveReservations:
            self.LLSFReservationsRXTuples.remove(toRemove)

    def _llsf_enable_blocked_reservationsRX(self, llsfUniqueID, reservationTuples):
        ''' Move LLSF reservations that were stored after receiving an ACK to the definite list of LLSF reservations. Only the ones in reservationTuples, delete the other ones.'''

        # 1) first check if we have to remove a reservation, b/c if there is already an enabled reservaton, the new one probably is an update
        self._llsf_remove_reservationsRX(llsfUniqueID, reservationTuples)

        # 2) now enable the new reservations
        toRemoveReservations = []
        enabledReservations = []
        for reservationTuple in self.blockedLLSFReservationsRXTuples:
            for resTuple in reservationTuples:
                if llsfUniqueID == reservationTuple[7] and resTuple[2] == reservationTuple[2] and resTuple[3] == reservationTuple[3] and resTuple[4] == reservationTuple[4]: # if startASN, endASN, interval are equal
                    self.LLSFReservationsRXTuples.append(reservationTuple) # enable it
                    enabledReservations.append(reservationTuple)
                    break # break if you've matched it against an 'allowed' reservation
            if llsfUniqueID == reservationTuple[7]: # anyway, remove it from the blocked list
                toRemoveReservations.append(reservationTuple)

        # the second case of this condition happens when a mote sent back an empty recurrent response (it did not accept any of the proposed reservations)
        # NOTE: when enabling that packet generation may change, this began to fail: my guess is that because when the mote receives a response, the reservation may be removed by the changing frequency and thus there can be a tuple but nothing anymore in the list
        # assert (len(toRemoveReservations) > 0 and len(reservationTuples) > 0) or (len(toRemoveReservations) == 0 and len(reservationTuples) == 0)
        # if not, something is wrong
        self._log(
            self.INFO,
            "[sixtop] On mote {0}, enabled from blockedLLSFReservationsRX: {1}. Removed from blocked: {2}",
            (self.id, str(enabledReservations), str(toRemoveReservations)),
        )
        for toRemove in toRemoveReservations:
            self.blockedLLSFReservationsRXTuples.remove(toRemove)

    def _llsf_remove_blocked_reservationsRX(self, llsfUniqueID):
        ''' Remove LLSF reservations that were stored after e.g., receiving a NACK.'''
        toRemoveReservations = []
        for reservationTuple in self.blockedLLSFReservationsRXTuples:
            if llsfUniqueID == reservationTuple[7]:
                toRemoveReservations.append(reservationTuple)

        # print 'On mote %d, removed from blockedLLSFReservationsRX: %s' % (self.id, str(toRemoveReservations))
        self._log(
            self.INFO,
            "[sixtop] On mote {0}, removed from blockedLLSFReservationsRX: {1} for llsfUniqueID {2}.",
            (self.id, str(toRemoveReservations), llsfUniqueID),
        )
        # NOTE: I commented this, because this could be triggered when a response is not ack'd, and in that that response actually no reservations were approved.
        # In this case you should allow this.
        #assert len(toRemoveReservations) > 0

        for toRemove in toRemoveReservations:
            self.blockedLLSFReservationsRXTuples.remove(toRemove)

    def _llsf_remove_reservationsTX(self, llsfUniqueID, reservationTuples):
        ''' Remove TX LLSF reservations that were enabled.'''
        toRemoveReservations = []
        for reservationTuple in self.LLSFReservationsTXTuples:
            if llsfUniqueID == reservationTuple[7]:
                toRemoveReservations.append(reservationTuple)

        self._log(
            self.INFO,
            "[sixtop] On mote {0}, removed from LLSFReservationsTX: {1}",
            (self.id, str(toRemoveReservations)),
        )
        for toRemove in toRemoveReservations:
            self.LLSFReservationsTXTuples.remove(toRemove)

    def _llsf_enable_blocked_reservationsTX(self, llsfUniqueID, reservationTuples):
        ''' Move LLSF reservations that were stored after sending an ADD to the definite list of LLSF reservations.'''

        # 1) first check if we have to remove a reservation, b/c if there is already an enabled reservaton, the new one probably is an update/new try
        self._llsf_remove_reservationsTX(llsfUniqueID, reservationTuples)

        # 2) now enable the new reservations
        toRemoveReservations = []
        enabledReservations = []
        for reservationTuple in self.blockedLLSFReservationsTXTuples:
            for resTuple in reservationTuples:
                # only enable those ones that are exactly the same
                if llsfUniqueID == reservationTuple[7] and resTuple[2] == reservationTuple[2] and resTuple[3] == reservationTuple[3] and resTuple[4] == reservationTuple[4]: # if startASN, endASN, interval are equal
                    self.LLSFReservationsTXTuples.append(reservationTuple)
                    enabledReservations.append(reservationTuple)
                    break
            if llsfUniqueID == reservationTuple[7]: # remove all tuples with the same transID
                toRemoveReservations.append(reservationTuple)

        #FIXME: got an error on this, that's way I deleted it. Maybe the problem is similar to the one in _llsf_enable_blocked_reservationsRX
        # the second case of this condition happens when a mote sent back an empty recurrent response (it did not accept any of the proposed reservations)
        # assert (len(toRemoveReservations) > 0 and len(reservationTuples) > 0) or (len(toRemoveReservations) == 0 and len(reservationTuples) == 0)
        # assert len(toRemoveReservations) > 0

        # if not, something is wrong
        for toRemove in toRemoveReservations:
            self.blockedLLSFReservationsTXTuples.remove(toRemove)
        self._log(
            self.INFO,
            "[sixtop] On mote {0}, enabled from blockedLLSFReservationsTX: {1}. Removed from blocked: {2}",
            (self.id, str(enabledReservations), str(toRemoveReservations)),
        )

    def _llsf_remove_blocked_reservationsTX(self, llsfUniqueID):
        ''' Remove LLSF reservations that were stored.'''
        toRemoveReservations = []
        for reservationTuple in self.blockedLLSFReservationsTXTuples:
            if llsfUniqueID == reservationTuple[7]:
                toRemoveReservations.append(reservationTuple)

        #FIXME: got an error on this, that's way I deleted it. Maybe the problem is similar to the one in _llsf_remove_blocked_reservationsRX
        # assert len(toRemoveReservations) > 0

        # if not, something is wrong
        self._log(
            self.INFO,
            "[sixtop] On mote {0}, removed from blockedLLSFReservationsTX: {1}",
            (self.id, str(toRemoveReservations)),
        )
        for toRemove in toRemoveReservations:
            self.blockedLLSFReservationsTXTuples.remove(toRemove)

    def _sixtop_action_receive_ADD(self, type, smac, payload):
        ''' got a ADD request from the neighbor. Handle it and respond.'''

        with self.dataLock:

            # parse ADD payload
            cells = payload[1] # cells to be reserved
            dirNeighbor = payload[2] # direction in which the cells should be reserved: TX or RX
            transID = payload[3] # 6P transaction identifier
            neighbor = smac # the originator of the message

            llsfPayloadOld = payload[4]
            llsfPayload = None
            newCells = []
            numCells = payload[5][0] # the number of cells the sender actually wants to reserve (incl. etx based cells)
            numCellsOrig = payload[5][1] # the original value of cells that you wanted to reserve without the etx
            llsfUniqueID = payload[6]
            thresholds = payload[7]

            # set direction of cells
            if dirNeighbor == self.DIR_TX:
                dir = self.DIR_RX
            else:
                dir = self.DIR_TX

            # this is coded like this to enable the frame switching
            selfSchedule = self.schedule
            neighborSchedule = neighbor.schedule
            # make sure that 6P reservations only happen in the normal frames
            if self.settings.switchFrameLengths:
                selfSchedule = self.defaultSchedule
            if self.settings.switchFrameLengths:
                neighborSchedule = neighbor.defaultSchedule

            if llsfPayloadOld is not None:
                okCells = 0
                asnToCollisionRate = []
                # 1) first collect all the startASNs and their collision rate
                for indexForThreshold, reservationTuple in enumerate(llsfPayloadOld):
                    numReservationASNs = self._llsf_get_numReservationASNs(reservationTuple[3], reservationTuple[4], reservationTuple[2])
                    assert numReservationASNs > 0

                    # initialize the collision solver
                    solver = CollisionSolver.CollisionSolver()

                    self._log(
                        self.INFO,
                        "[sixtop] On mote {0}, checking for tuple: {1} and llsfUniqueID {2}",
                        (self.id, reservationTuple, llsfUniqueID),
                    )

                    # 1.1) get the collisions with enabled/blocked reservations
                    collisions = []
                    for resTuple in self.LLSFReservationsRXTuples:
                        collisions += solver.getCollisions(reservationTuple[3], reservationTuple[4], reservationTuple[2], resTuple[3], resTuple[4], resTuple[2])
                    for resTuple in self.LLSFReservationsTXTuples:
                        collisions += solver.getCollisions(reservationTuple[3], reservationTuple[4], reservationTuple[2], resTuple[3], resTuple[4], resTuple[2])
                    for resTuple in self.blockedLLSFReservationsRXTuples:
                        collisions += solver.getCollisions(reservationTuple[3], reservationTuple[4], reservationTuple[2], resTuple[3], resTuple[4], resTuple[2])
                    for resTuple in self.blockedLLSFReservationsTXTuples:
                        collisions += solver.getCollisions(reservationTuple[3], reservationTuple[4], reservationTuple[2], resTuple[3], resTuple[4], resTuple[2])

                    # 1.2) get the collisions with the shared reservations
                    sharedTSs = [ts for (ts, ch_shared, nMote) in self.getSharedCells()]
                    sharedCollisions = []
                    for sharedTS in sharedTSs:
                        sharedCollisions += solver.getCollisions(reservationTuple[3], reservationTuple[4], reservationTuple[2], sharedTS, reservationTuple[4], self.settings.slotframeLength) # define the shared cells also as tuples.
                        collisions += sharedCollisions

                    uniqueCollisions = set(collisions)
                    lenUniqueCollisions = len(uniqueCollisions)

                    collisionRate = lenUniqueCollisions / float(numReservationASNs)
                    collisionRate = float(format(collisionRate, '.2f')) # rounded to two decimals
                    asnToCollisionRate += [(reservationTuple, collisionRate, lenUniqueCollisions)]

                # sort them based on the collision rate
                sortedASNToCollisionRate = sorted(asnToCollisionRate, key=lambda x: (x[1],x[0]))
                # print 'SORTED collision rate map: %s' % (str(sortedASNToCollisionRate))

                # 2) reserve the cells
                for index in range(0, min(numCells, len(sortedASNToCollisionRate))):
                    reservationTuple = sortedASNToCollisionRate[index][0]
                    # block the reservations
                    self.blockedLLSFReservationsRXTuples.append((reservationTuple[0], reservationTuple[1], reservationTuple[2], reservationTuple[3], reservationTuple[4], transID, smac, llsfUniqueID))
                    # print 'Approved this one: %s' % str(reservationTuple)
                    self._log(
                        self.INFO,
                        "[sixtop] Approved this one {0} with collision rate {1} and number of collisions {2}.",
                        (str(reservationTuple), sortedASNToCollisionRate[index][1], sortedASNToCollisionRate[index][2]),
                    )
                    # log the possible RX collisions
                    self.engine.scheduledPossibleRXRecurrentCollisions += sortedASNToCollisionRate[index][2]

                    if llsfPayload == None:
                        llsfPayload = []
                    llsfPayload.append(reservationTuple) # return only the tuple that is used to do the reservation

                self._log(
                    self.INFO,
                    "[sixtop] On mote {0}, approved these reservation tuples: {1} (llsfUniqueID {2}).",
                    (self.id, str(llsfPayload), llsfUniqueID),
                )

            # if llsfPayloadOld is not None:
            #     # print 'I have to reserve %d cells.' % numCells
            #     okCells = 0
            #     for indexForThreshold, reservationTuple in enumerate(llsfPayloadOld):
            #         reservationASNs = self._llsf_get_reservationASNs(reservationTuple[3], reservationTuple[4], reservationTuple[2])
            #         assert len(reservationASNs) > 0
            #         sorted(reservationASNs)
            #
            #         COLLISION_THRESHOLD = self.settings.collisionRateThreshold
            #         # if thresholds is None or (len(thresholds) - 1) < indexForThreshold:
            #         #     raise -1
            #         # else:
            #         #     COLLISION_THRESHOLD = thresholds[indexForThreshold]
            #
            #         if len(reservationASNs) <= 3 and COLLISION_THRESHOLD < 0.5:
            #             COLLISION_THRESHOLD = 0.5
            #
            #         solver = CollisionSolver.CollisionSolver()
            #         self._log(
            #             self.INFO,
            #             "[sixtop] On mote {0}, checking for tuple: {1} and llsfUniqueID {2}",
            #             (self.id, reservationTuple, llsfUniqueID),
            #         )
            #         # TODO:  for updates I should find another way of not having collisions.
            #         # if I include the llsfUniqueID != resTuple[7], it prohibits nodes finding reservations for example
            #         # node x wants to send a reservation to its parent (to be put in LLSFReservationsTXTuples), but the reservation formula should not match with the reservation coming from his child (LLSFReservationsRXTuples)
            #         # this reservations will have the same ID, so the collisions are not calculated
            #         collisions = []
            #         for resTuple in self.LLSFReservationsRXTuples:
            #             # if llsfUniqueID != resTuple[7]: # if there is already a reservation with this ID, the new one is probably an update
            #             collisions += solver.getCollisions(reservationTuple[3], reservationTuple[4], reservationTuple[2], resTuple[3], resTuple[4], resTuple[2])
            #             # lenUniqueCollisions = len(set(collisions))
            #             # self._log(
            #             #     self.INFO,
            #             #     "[sixtop] On mote {0}, llsf rx tuples collision after collision solver: {1}. tuple {2}",
            #             #     (self.id, lenUniqueCollisions, resTuple),
            #             # )
            #         for resTuple in self.LLSFReservationsTXTuples:
            #             # if llsfUniqueID != resTuple[7]: # if there is already a reservation with this ID, the new one is probably an update
            #             collisions += solver.getCollisions(reservationTuple[3], reservationTuple[4], reservationTuple[2], resTuple[3], resTuple[4], resTuple[2])
            #             # lenUniqueCollisions = len(set(collisions))
            #             # self._log(
            #             #     self.INFO,
            #             #     "[sixtop] On mote {0}, llsf tx tuples collision after collision solver: {1}. tuple {2}",
            #             #     (self.id, lenUniqueCollisions, resTuple),
            #             # )
            #         for resTuple in self.blockedLLSFReservationsRXTuples:
            #             # if llsfUniqueID != resTuple[7]: # if there is already a reservation with this ID, the new one is probably an update
            #             collisions += solver.getCollisions(reservationTuple[3], reservationTuple[4], reservationTuple[2], resTuple[3], resTuple[4], resTuple[2])
            #             # lenUniqueCollisions = len(set(collisions))
            #             # self._log(
            #             #     self.INFO,
            #             #     "[sixtop] On mote {0}, llsf blocked rx tuples collision after collision solver: {1}. tuple {2}",
            #             #     (self.id, lenUniqueCollisions, resTuple),
            #             # )
            #         for resTuple in self.blockedLLSFReservationsTXTuples:
            #             # if llsfUniqueID != resTuple[7]: # if there is already a reservation with this ID, the new one is probably an update
            #             collisions += solver.getCollisions(reservationTuple[3], reservationTuple[4], reservationTuple[2], resTuple[3], resTuple[4], resTuple[2])
            #             # lenUniqueCollisions = len(set(collisions))
            #             # self._log(
            #             #     self.INFO,
            #             #     "[sixtop] On mote {0}, llsf blocked tx tuples collision after collision solver: {1}. tuple {2}",
            #             #     (self.id, lenUniqueCollisions, resTuple),
            #             # )
            #
            #         # make the collisions unique (normally they should be unique)
            #         uniqueCollisions = set(collisions)
            #         lenUniqueCollisions = len(uniqueCollisions)
            #         self._log(
            #             self.INFO,
            #             "[sixtop] On mote {0}, collision after collision solver: {1}.",
            #             (self.id, lenUniqueCollisions),
            #         )
            #         reservationASNsToTSs = [resASN % self.settings.slotframeLength for resASN in reservationASNs]
            #         # print 'On mote %d, the recurrent reservations to timeslots: %s.' % (self.id, reservationASNsToTSs)
            #         self._log(
            #             self.INFO,
            #             "[sixtop] On mote {0}, the recurrent reservations to timeslots: {1}.",
            #             (self.id, reservationASNsToTSs),
            #         )
            #         # only look at a buffer period, because otherwise it will be very difficult to find a cell
            #         # bufferEndASN = self.engine.asn + self.bufferPeriod
            #
            #         # also look for collisions with normal resevations
            #
            #         # normalAndSharedCells = [ts for (ts, cell) in self.schedule.iteritems()]
            #         # # print 'On mote %d, normal and shared slots: %s.' % (self.id, normalAndSharedCells)
            #         # self._log(
            #         #     self.INFO,
            #         #     "[sixtop] On mote {0}, normal and shared slots: {1}.",
            #         #     (self.id, normalAndSharedCells),
            #         # )
            #         sharedTSs = [ts for (ts, ch_shared, nMote) in self.getSharedCells()]
            #         # print normalAndSharedCells
            #         for resASN in reservationASNs: # also check how many collide with SHARED slots
            #             # if resASN <= bufferEndASN:
            #             ts = resASN % self.settings.slotframeLength
            #             if ts in sharedTSs: # if this timeslot is in the ts, increment the number of collisions
            #                 lenUniqueCollisions += 1
            #                 # print 'Collision at ts %d' % ts
            #
            #         colrate = lenUniqueCollisions / float(len(reservationASNs))
            #
            #         if colrate not in self.engine.collisionPercentages:
            #             self.engine.collisionPercentages.append(colrate)
            #
            #         self._log(
            #             self.INFO,
            #             '[6top] On mote {0}, collision rate is: {1} (in receive)',
            #             (str(self.id), colrate),
            #         )
            #
            #         # while colrate > COLLISION_THRESHOLD:
            #         #     COLLISION_THRESHOLD += self.settings.collisionRateThreshold
            #         #     if COLLISION_THRESHOLD > thresholds[indexForThreshold]: # if you exceed the passed threshold, break
            #         #         break
            #
            #         if lenUniqueCollisions / float(len(reservationASNs)) > COLLISION_THRESHOLD:
            #             # print 'On mote %d, skip reservation tuple %s.' % (self.id, reservationTuple)
            #             continue # collision rate is too high, break
            #         else:
            #             # block the reservations
            #             self.blockedLLSFReservationsRXTuples.append((reservationTuple[0], reservationTuple[1], reservationTuple[2], reservationTuple[3], reservationTuple[4], transID, smac, llsfUniqueID))
            #             # print 'Approved this one: %s' % str(reservationTuple)
            #             self._log(
            #                 self.INFO,
            #                 "[sixtop] Approved this one: {0}.",
            #                 (str(reservationTuple)),
            #             )
            #             # log the possible RX collisions
            #             self.engine.scheduledPossibleRXRecurrentCollisions += lenUniqueCollisions
            #
            #             if llsfPayload == None:
            #                 llsfPayload = []
            #             llsfPayload.append(reservationTuple) # return only the tuple that is used to do the reservation
            #             okCells +=  1
            #             if okCells == numCells:
            #                 break # break, because you've found a reservation
            #
            #     self._log(
            #         self.INFO,
            #         "[sixtop] On mote {0}, approved these reservation tuples: {1} (llsfUniqueID {2}).",
            #         (self.id, str(llsfPayload), llsfUniqueID),
            #     )
            else:
                # make a list of all the timeslots that are blocked for other other operations
                blockedCellListTSs = [ts for (ts, ch) in self.blockedCellList]
                # gdaneels, NOTE: this can stay slotframelength because 6P only does reservations in normal slotframes
                availableTimeslots = list(set(range(self.settings.slotframeLength)) - set(selfSchedule.keys()) - set(blockedCellListTSs))
                if self.settings.sf == 'chang' or self.settings.sf == 'recurrent_chang':
                    recurrentReservationTimeslots = self._sixtop_cell_get_all_recurrent_timeslots()
                    availableTimeslots = list(set(range(self.settings.slotframeLength)) - set(selfSchedule.keys()) - set(blockedCellListTSs) - recurrentReservationTimeslots)

                availableTimeslots    = sorted(availableTimeslots)

                cellList = []
                newCells = []
                for cell in cells:
                    ts = cell[0] # timeslot is the first element
                    ch = cell[1] # channel is the second element
                    if ts in availableTimeslots:
                        cellList += [(ts,ch,dir)]
                        newCells += [(ts,ch)]
                        if len(newCells) == numCells:
                            break # only reserve so many cells as requested, there are always more given

                # block these cells so in other sixtop operations, no one can use these cells
                self._tsch_blockCells(newCells)

            if llsfPayload is None and llsfPayloadOld is not None:
                llsfPayload = [] # no appropriate cell was found, change this to [] b/c the llsfPayload being None would suggest it is not a recurrent reservation response while it is
            self._sixtop_action_enqueue_RESPONSE(newCells, neighbor, transID, dir, llsfPayload = llsfPayload, numCells=(numCells, numCellsOrig), llsfUniqueID=llsfUniqueID)

    def _sixtop_action_addCells_RESPONSE(self, packet):

        # (ts, ch) format
        cells = packet['payload'][1]
        dir = packet['payload'][3]
        neighbor = packet['destination']

        # this is coded like this to enable the frame switching
        selfSchedule = self.schedule
        neighborSchedule = neighbor.schedule
        # make sure that 6P reservations only happen in the normal frames
        if self.settings.switchFrameLengths:
            selfSchedule = self.defaultSchedule
        if self.settings.switchFrameLengths:
            neighborSchedule = neighbor.defaultSchedule

        cellList = []
        for ts, ch in cells:
            # log
            self._log(
                self.INFO,
                '[6top] add RX cell ts={0},ch={1} from {2} to {3}',
                (ts,ch,self.id,neighbor.id),
            )
            cellList         += [(ts,ch,dir)]
        self._tsch_addCells(neighbor,cellList,selfSchedule)

        # update counters
        if dir==self.DIR_TX:
            if neighbor not in self.numCellsToNeighbors:
                self.numCellsToNeighbors[neighbor]     = 0
            self.numCellsToNeighbors[neighbor]        += len(cells)
        else:
            if neighbor not in self.numCellsFromNeighbors:
                self.numCellsFromNeighbors[neighbor]   = 0
            self.numCellsFromNeighbors[neighbor]      += len(cells)

    def _sixtop_cell_deletion_sender(self,neighbor,tsList):
        with self.dataLock:

            if neighbor in self.sixtopDeletionTX and self.sixtopDeletionTX[neighbor][2] > 0: # if there is still a deletion ongoing, abort
                return False

            # print 'On mote %d, in deletion_sender for neighbor %d with list: %s' % (self.id, neighbor.id, str(tsList))
            deleteID = self._sixtop_action_enqueue_DELETE(tsList, neighbor)
            self.sixtopDeletionTX[neighbor] = (deleteID, tsList, self.sixtop_timout_init)

            # log
            self._log(
                self.INFO,
                "[6top] remove timeslots={0} with {1}",
                (tsList,neighbor.id),
            )

            # NOTE: this is the code to delete a cell without a packet!
            # NOTE: if you enable this, you probably should disable the code above
            # self._tsch_removeCells(
            #     neighbor     = neighbor,
            #     tsList       = tsList,
            # )
            # neighbor._sixtop_cell_deletion_receiver(self,tsList)
            # self.numCellsToNeighbors[neighbor]       -= len(tsList)
            # assert self.numCellsToNeighbors[neighbor]>=0

    def _sixtop_cell_deletion_receiver(self,neighbor,tsList):
        with self.dataLock:
            self._tsch_removeCells(
                neighbor     = neighbor,
                tsList       = tsList,
            )
            self.numCellsFromNeighbors[neighbor]     -= len(tsList)
            assert self.numCellsFromNeighbors[neighbor]>=0

    def _sixtop_action_receive_DELETE(self,type,smac,payload):
        ''' received a 6P DELETE request message. '''
        with self.dataLock:
            tsList = payload[1] # cells to be reserved
            deleteID = payload[2] # 6P transaction identifier
            neighbor = smac # the originator of the message

            # NOTE: this has to be moved to the action_removeCells_DELETE_RESPONSE
            # if self.settings.switchFrameLengths: # only delete from default schedule
            #     self._tsch_removeCells(
            #         neighbor     = neighbor,
            #         tsList       = tsList,
            #         schedule     = self.defaultSchedule,
            #     )
            # else:
            #     self._tsch_removeCells(
            #         neighbor     = neighbor,
            #         tsList       = tsList,
            #     )
            # self.numCellsFromNeighbors[neighbor]     -= len(tsList)
            # assert self.numCellsFromNeighbors[neighbor]>=0
            # for n, cells in self.numCellsFromNeighbors.iteritems():
            #     print n.id

            self._sixtop_action_enqueue_DELETE_RESPONSE(tsList, neighbor, deleteID)

    def _sixtop_action_receive_DELETE_RESPONSE(self,type,smac,payload):
        ''' received a 6P DELETE RESPONSE message. '''
        with self.dataLock:
            # get the necessary information
            neighbor = smac
            tsList       = payload[1]
            deleteID     = payload[2]

            # or the last deletion expired and there is no new one
            # or the last deletion expired and there is already a new one
            # = this is probably a RESPONSE to old DELETE, discard it
            if smac not in self.sixtopDeletionTX or deleteID != self.sixtopDeletionTX[smac][0]:
                # print '--------------------+++------------------------------'
                return False # the message can not be ack'd

            # TODO: this should always be true?
            assert deleteID == self.sixtopDeletionTX[smac][0]

            if self.settings.switchFrameLengths: # only delete from default schedule
                self._tsch_removeCells(
                    neighbor     = neighbor,
                    tsList       = tsList,
                    schedule     = self.defaultSchedule,
                )
            else:
                self._tsch_removeCells(
                    neighbor     = neighbor,
                    tsList       = tsList,
                )

            # print 'tsList: %s' % str(tsList)
            # print 'defaultSchedule: %s' % str(self.defaultSchedule.keys())
            # print 'mgmtSchedule: %s' % str(self.mgmtSchedule.keys())

            self.numCellsToNeighbors[neighbor]       -= len(tsList)
            assert self.numCellsToNeighbors[neighbor]>=0

            return True # the message can be ack'd

    def _sixtop_action_removeCells_DELETE_RESPONSE(self, packet):
        ''' delete cells at receiver after receiving the ACK to the DELETE RESPONSE msg '''
        with self.dataLock:

            # element 1 of payload contains the remove list
            tsList = packet['payload'][1]
            # print tsList
            # destination contains the remove list
            neighbor = packet['destination']

            if self.settings.switchFrameLengths: # only delete from default schedule
                self._tsch_removeCells(
                    neighbor     = neighbor,
                    tsList       = tsList,
                    schedule     = self.defaultSchedule,
                )
            else:
                self._tsch_removeCells(
                    neighbor     = neighbor,
                    tsList       = tsList,
                )

            self.numCellsFromNeighbors[neighbor]       -= len(tsList)
            assert self.numCellsFromNeighbors[neighbor]>=0

    def _sixtop_removeCells(self,neighbor,numCellsToRemove):
        '''
        Finds cells to neighbor, and remove it.
        '''
        # get cells to the neighbors
        scheduleList = []

        if self.settings.sf == 'chang' or self.settings.sf == 'recurrent_chang': # CHANG's code, slightly tweaked
            rxTimeslots = None
            txTimeslots = None
            if neighbor == self.preferredParent:
                rxTimeslots = [ts for (ts,cell) in self.schedule.iteritems() if cell['dir']==self.DIR_RX and cell['neighbor']!=self.preferredParent and not cell['llsfCell']]
                txTimeslots = [ts for (ts,cell) in self.schedule.iteritems() if cell['dir']==self.DIR_TX and cell['neighbor']==neighbor and not cell['llsfCell']]
            else: # it is a child?
                rxTimeslots = [ts for (ts,cell) in self.schedule.iteritems() if cell['dir']==self.DIR_RX and cell['neighbor']==neighbor and not cell['llsfCell']]
                txTimeslots = [ts for (ts,cell) in self.schedule.iteritems() if cell['dir']==self.DIR_TX and cell['neighbor']==neighbor and not cell['llsfCell']]

            txTsDistance = []
            for i in range(len(txTimeslots)):
                distance = self.settings.slotframeLength
                for j in range(len(rxTimeslots)):
                    if txTimeslots[i] > rxTimeslots[j]:
                        if txTimeslots[i] - rxTimeslots[j] > distance:
                            distance = txTimeslots[i] - rxTimeslots[j]
                    else:
                        if self.settings.slotframeLength + txTimeslots[i] - rxTimeslots[j] > distance:
                            distance = self.settings.slotframeLength + txTimeslots[i] - rxTimeslots[j]
                txTsDistance.append((txTimeslots[i],distance))
            # print txTsDistance
            txTsDistance.sort(key=lambda x: x[1], reverse=True)
            # print txTsDistance

            scheduleList = []
            for i in range(len(txTsDistance)):
                numTxAck = self.schedule[txTsDistance[i][0]]['numTxAck']
                numTx    = self.schedule[txTsDistance[i][0]]['numTx']
                cellPDR  = (float(numTxAck)+(self.getPDR(neighbor)*self.NUM_SUFFICIENT_TX))/(numTx+self.NUM_SUFFICIENT_TX)
                scheduleList += [(txTsDistance[i][0],numTxAck,numTx,cellPDR)]

        else:

            # worst cell removing initialized by theoretical pdr
            for (ts,cell) in self.schedule.iteritems():
                if cell['neighbor']==neighbor and cell['dir']==self.DIR_TX and not cell['llsfCell']:
                    cellPDR           = (float(cell['numTxAck'])+(self.getPDR(neighbor)*self.NUM_SUFFICIENT_TX))/(cell['numTx']+self.NUM_SUFFICIENT_TX)
                    scheduleList     += [(ts,cell['numTxAck'],cell['numTx'],cellPDR)]

            # introduce randomness in the cell list order
            self.randomGen.shuffle(scheduleList)

            if not self.settings.sixtopNoRemoveWorstCell:
                # triggered only when worst cell selection is due
                # (cell list is sorted according to worst cell selection)
                scheduleListByPDR     = OrderedDict()
                for tscell in scheduleList:
                    if not scheduleListByPDR.has_key(tscell[3]):
                        scheduleListByPDR[tscell[3]]=[]
                    scheduleListByPDR[tscell[3]]+=[tscell]
                # print 'hereeeee worst cell?'
                rssi                  = self.getRSSI(neighbor)
                theoPDR               = Topology.Topology.rssiToPdr(rssi)
                scheduleList          = []
                for pdr in sorted(scheduleListByPDR.keys()):
                    if pdr<theoPDR:
                        scheduleList += sorted(scheduleListByPDR[pdr], key=lambda x: x[2], reverse=True)
                    else:
                        scheduleList += sorted(scheduleListByPDR[pdr], key=lambda x: x[2])

        # remove a given number of cells from the list of available cells (picks the first numCellToRemove)
        tsList=[]
        for tscell in scheduleList[:numCellsToRemove]:

            # log
            self._log(
                self.INFO,
                "[otf] remove cell ts={0} to {1} (pdr={2:.3f})",
                (tscell[0],neighbor.id,tscell[3]),
            )
            tsList += [tscell[0]]

        # remove cells
        self._sixtop_cell_deletion_sender(neighbor,tsList)

    #===== tsch

    # remove the last data packet from the queue
    # this will be probably be replaced with a mgmt packet
    def _tsch_removeFirstPacket(self):
        for pkt in self.txQueue:
            if pkt['type'] == self.APP_TYPE_DATA:
                pktToRemove = pkt
        if pktToRemove != None:
            self.txQueue.remove(pktToRemove)

    def _tsch_enqueue(self,packet):

        if not (self.preferredParent or self.dagRoot):
            # I don't have a route

            # increment mote state
            self._stats_incrementMoteStats('droppedNoRoute')

            return False

        elif not (self.getTxCellsNeighbor(self.preferredParent) or self.getSharedCells()):
            # I don't have any transmit cells

            # # increment mote state
            # self._stats_incrementMoteStats('droppedNoTxCells')
            #
            # return False

            # gdaneels, makes more sense
            if len(self.txQueue)==self.TSCH_QUEUE_SIZE:
                # my TX queue is full

                # increment mote state
                self._stats_incrementMoteStats('droppedNoTxCells')
                # update mote stats
                self._stats_incrementMoteStats('droppedQueueFull')

                return False
            else:
                # increment mote state
                self._stats_incrementMoteStats('notDroppedNoTxCells')

                # print 'on mote %s, enqueued new packet' % (str(self.id))
                #
                # print 'putting it in the queue, no tx cells'

                self.txQueue    += [packet]
                return True

        elif len(self.txQueue)==self.TSCH_QUEUE_SIZE:
            # my TX queue is full

            # update mote stats
            self._stats_incrementMoteStats('droppedQueueFull')

            return False

        else:
            # all is good

            # enqueue packet
            self.txQueue    += [packet]

            return True

    def _tsch_enqueue_relayed(self,packet):

        if not (self.preferredParent or self.dagRoot):
            # I don't have a route

            # increment mote state
            self._stats_incrementMoteStats('droppedNoRouteRelayed')

            return False

        elif not (self.getTxCellsNeighbor(self.preferredParent) or self.getSharedCells()):
            # I don't have any transmit cells

            # # increment mote state
            # self._stats_incrementMoteStats('droppedNoTxCells')
            #
            # return False

            # gdaneels, makes more sense
            if len(self.txQueue)==self.TSCH_QUEUE_SIZE:
                # my TX queue is full

                # increment mote state
                self._stats_incrementMoteStats('droppedNoTxCellsRelayed')
                # update mote stats
                self._stats_incrementMoteStats('droppedQueueFullRelayed')

                return False
            else:
                # increment mote state
                self._stats_incrementMoteStats('notDroppedNoTxCellsRelayed')

                # print 'on mote %s, enqueued new packet' % (str(self.id))
                #
                # print 'putting it in the queue, no tx cells'

                self.txQueue    += [packet]
                return True

        elif len(self.txQueue)==self.TSCH_QUEUE_SIZE:
            # my TX queue is full

            # update mote stats
            self._stats_incrementMoteStats('droppedQueueFullRelayed')

            return False

        else:
            # all is good

            # enqueue packet
            self.txQueue    += [packet]

            return True

    def _tsch_enqueue_neighbor(self,packet,neighbor=None):
        ''' check if a particular neighbor has cells or if there are shared cells. '''

        # TODO: when sending packets to your children, of course you do not need a preferred parent or be the dagroot
        # but because I assume you always have a preferred parent, I just let this condition be
        if not (self.preferredParent or self.dagRoot):
            # I don't have a route

            # increment mote state
            self._stats_incrementMoteStats('droppedNoRouteTop')
            raise -1
            return False

        elif not (self.getTxCellsNeighbor(neighbor) or self.getSharedCells()):
            # I don't have any transmit cells

            # # increment mote state
            # self._stats_incrementMoteStats('droppedNoTxCells')
            #
            # return False

            # gdaneels, makes more sense
            if len(self.txQueue)==self.TSCH_QUEUE_SIZE:
                # my TX queue is full

                # increment mote state
                self._stats_incrementMoteStats('droppedNoTxCellsTop')
                # update mote stats
                self._stats_incrementMoteStats('droppedQueueFullTop')
                return False
            else:
                # increment mote state
                self._stats_incrementMoteStats('notDroppedNoTxCellsTop')

                # print 'on mote %s, enqueued new packet' % (str(self.id))
                #
                # print 'putting it in the queue, no tx cells'

                self.txQueue    += [packet]
                return True

        elif len(self.txQueue)==self.TSCH_QUEUE_SIZE:
            # my TX queue is full

            pktToRemove = None
            # if it is full, remove one data packet
            if len(self.txQueue)==self.TSCH_QUEUE_SIZE:
                for pkt in self.txQueue:
                    if pkt['type'] == self.APP_TYPE_DATA:
                        pktToRemove = pkt
                if pktToRemove != None:
                    self.txQueue.remove(pktToRemove)
                    self.txQueue    += [packet]
                    return True
                else:
                    # update mote stats
                    self._stats_incrementMoteStats('droppedQueueFullTop')
                    return False

        else:
            # all is good
            # enqueue packet
            self.txQueue    += [packet]

            return True

    def _tsch_schedule_activeCell(self):
        asn        = self.engine.getAsn()
        tsCurrent = None
        if self.settings.switchFrameLengths:
            tsCurrent = self.engine.getTimeslot()
        else:
            tsCurrent  = asn%self.settings.slotframeLength

        tsDiffMin             = None

        # find closest active slot in schedule
        with self.dataLock:

            if not self.schedule:
                self.engine.removeEvent(uniqueTag=(self.id,'_tsch_action_activeCell'))
                return

            if self.settings.switchFrameLengths:
                currentFrameType = self.currentScheduleName
                # print 'on mote %d, current frame type %s' % (self.id, currentFrameType)
                nextFrameType = self.engine.getNextFrameType()
                # print 'on mote %d, next frame type %s' % (self.id, nextFrameType)

                # if the current and next frame types are the same, just do the same as always
                if currentFrameType == nextFrameType:
                    # print 'Come here? at ASN %d at mote %d' % (self.engine.asn, self.id)
                    tsDiffMin             = None
                    for (ts,cell) in self.schedule.items():
                        if ts==tsCurrent:
                            tsDiff        = self.settings.slotframeLength
                        elif ts>tsCurrent:
                            tsDiff        = ts-tsCurrent
                        elif ts<tsCurrent:
                            tsDiff        = (ts+self.settings.slotframeLength)-tsCurrent
                        else:
                            raise SystemError()

                        if (not tsDiffMin) or (tsDiffMin>tsDiff):
                            tsDiffMin     = tsDiff
                elif currentFrameType != nextFrameType:
                    # print 'Come here at different frame types? at ASN %d at mote %d' % (self.engine.asn, self.id)
                    currentSchedule = None
                    nextSchedule = None
                    if currentFrameType == 'mgmt' and nextFrameType == 'default':
                        # print 'Come here ------'
                        currentSchedule = self.mgmtSchedule
                        nextSchedule = self.defaultSchedule
                    elif currentFrameType == 'default' and nextFrameType == 'mgmt':
                        currentSchedule = self.defaultSchedule
                        nextSchedule = self.mgmtSchedule
                    else:
                        assert False

                    tsDiff = None

                    # get the keys from the current schedule
                    keysCurrentSchedule = list(currentSchedule.keys())
                    # print 'keysCurrentSchedule'
                    # print keysCurrentSchedule
                    # get the keys from the next schedule
                    keysNextSchedule = list(nextSchedule.keys())
                    # print 'keysNextSchedule'
                    # print keysNextSchedule

                    for ts in keysCurrentSchedule:
                        if ts > tsCurrent and (tsDiff == None or (ts-tsCurrent < tsDiff)):
                            tsDiff = ts - tsCurrent

                    # print tsDiff

                    # if you did not find a slot in the current schedule, go to the next
                    if tsDiff == None:
                        ts = min(keysNextSchedule) # take the smallest key
                        # print 'min is %d on mote %d' % (ts, self.id)
                        assert ts != None
                        # print self.settings.currentFrameLength
                        # print tsCurrent
                        # print ts
                        tsDiff = (self.settings.currentFrameLength - tsCurrent) + ts

                    tsDiffMin = tsDiff

                    # print 'at ASN %d on mote %d: tsDiffmin %d' % (self.engine.asn, self.id, tsDiffMin)

            else:
                tsDiffMin             = None
                for (ts,cell) in self.schedule.items():
                    if ts==tsCurrent:
                        tsDiff        = self.settings.slotframeLength
                    elif ts>tsCurrent:
                        tsDiff        = ts-tsCurrent
                    elif ts<tsCurrent:
                        tsDiff        = (ts+self.settings.slotframeLength)-tsCurrent
                    else:
                        raise SystemError()

                    if (not tsDiffMin) or (tsDiffMin>tsDiff):
                        tsDiffMin     = tsDiff

        # print 'Come in schedule activeCell at mote %d at ASN %d and scheduling a activeCell in %d slots.' % (self.id, asn, tsDiffMin)

        # schedule at that ASN
        self.engine.scheduleAtAsn(
            asn         = asn+tsDiffMin,
            cb          = self._tsch_action_activeCell,
            uniqueTag   = (self.id,'_tsch_action_activeCell'),
            priority    = 0,
        )

    def _check_for_only_data_in_queueu(self):
        for pkt in self.txQueue:
            if pkt['type'] != self.APP_TYPE_DATA:
                return False
        return True

    def _tsch_action_activeCell(self):
        '''
        active slot starts. Determine what todo, either RX or TX, use the propagation model to introduce
        interference and Rx packet drops.
        '''

        asn = self.engine.getAsn()
        ts = None
        if self.settings.switchFrameLengths:
            ts = self.engine.getTimeslot()
        else:
            ts  = asn%self.settings.slotframeLength

        with self.dataLock:

            # if ts not in self.schedule:
            #     print 'on mote %d: %d' % (self.id, ts)
            # make sure this is an active slot
            assert ts in self.schedule
            # make sure we're not in the middle of a TX/RX operation
            assert not self.waitingFor

            cell = self.schedule[ts]

            if  cell['dir']==self.DIR_RX:
                self.engine.logActiveRXCell(self.id)
                # start listening
                self.propagation.startRx(
                    mote          = self,
                    channel       = cell['ch'],
                )

                # indicate that we're waiting for the RX operation to finish
                self.waitingFor   = self.DIR_RX

            elif cell['dir']==self.DIR_TX:
                self.engine.logActiveTXCell(self.id)
                # check whether packet to send
                self.pktToSend = None
                if self.txQueue:
                    for pkt in self.txQueue:
                        # NOTE: data packets should only be sent in cells towards the preferred parent! Otherwise you can end up looping a packet.
                        if pkt['type'] == self.APP_TYPE_DATA and cell['neighbor'].id == self.preferredParent.id:
                            self.pktToSend = pkt # do not break here, because other packets should get preference
                            if self._check_for_only_data_in_queueu():
                                # if there are only data packets in the queue you should break, because you need to take the first data packet!
                                # if there are also other packets, take the others first because mgmt gets precedence
                                break
                        elif pkt['type'] == self.SIXTOP_TYPE_ADD and self.settings.sixtopInTXRX and cell['neighbor'].id == pkt['destination'].id:
                            self.pktToSend = pkt
                            break
                        elif pkt['type'] == self.SIXTOP_TYPE_RESPONSE and self.settings.sixtopInTXRX and cell['neighbor'].id == pkt['destination'].id:
                            self.pktToSend = pkt
                            break
                        elif pkt['type'] == self.SIXTOP_TYPE_DELETE and self.settings.sixtopInTXRX and cell['neighbor'].id == pkt['destination'].id:
                            self.pktToSend = pkt
                            break
                        elif pkt['type'] == self.SIXTOP_TYPE_DELETE_RESPONSE and self.settings.sixtopInTXRX and cell['neighbor'].id == pkt['destination'].id:
                            self.pktToSend = pkt
                            break

                # send packet
                if self.pktToSend:

                    cell['numTx'] += 1

                    self.propagation.startTx(
                        channel   = cell['ch'],
                        type      = self.pktToSend['type'],
                        smac      = self,
                        dmac      = [cell['neighbor']],
                        payload   = self.pktToSend['payload'],
                    )

                    # indicate that we're waiting for the TX operation to finish
                    self.waitingFor   = self.DIR_TX

                    # log charge usage
                    self._logChargeConsumed(self.CHARGE_TxDataRxAck_uC)

            elif cell['dir']==self.DIR_TXRX_SHARED:
                self.engine.logActiveSharedCell(self.id)
                if self.settings.switchFrameLengths:
                    self.mgmtSlotsToSubstract += 1 # this is necessary for the OTF calculation

                # only send a mgmt packet in a SHARED slot if you do NOT have ANY dedicated cell to this neighbor
                self.pktToSend = None
                if self.txQueue:
                    for pkt in self.txQueue:
                        if pkt['type'] == self.RPL_TYPE_DIO:
                            self.pktToSend = pkt
                            break
                        elif pkt['type'] == self.SIXTOP_TYPE_ADD and pkt['nextTX'] == 1 and len(self.getTxCellsNeighbor(pkt['destination'])) == 0 and self.settings.sixtopInSHARED:
                            self.pktToSend = pkt
                            break
                        elif pkt['type'] == self.SIXTOP_TYPE_RESPONSE and pkt['nextTX'] == 1 and len(self.getTxCellsNeighbor(pkt['destination'])) == 0 and self.settings.sixtopInSHARED:
                            self.pktToSend = pkt
                            break
                        elif pkt['type'] == self.SIXTOP_TYPE_DELETE and pkt['nextTX'] == 1 and len(self.getTxCellsNeighbor(pkt['destination'])) == 0 and self.settings.sixtopInSHARED:
                            self.pktToSend = pkt
                            break
                        elif pkt['type'] == self.SIXTOP_TYPE_DELETE_RESPONSE and pkt['nextTX'] == 1 and len(self.getTxCellsNeighbor(pkt['destination'])) == 0 and self.settings.sixtopInSHARED:
                            self.pktToSend = pkt
                            break

                    # update the nextTX for the SALOHA
                    for pkt in self.txQueue:
                        if pkt['type'] == self.SIXTOP_TYPE_ADD and pkt['nextTX'] > 1 and self.settings.sixtopInSHARED:
                            pkt['nextTX'] -= 1
                        elif pkt['type'] == self.SIXTOP_TYPE_RESPONSE and pkt['nextTX'] > 1 and self.settings.sixtopInSHARED:
                            pkt['nextTX'] -= 1
                        elif pkt['type'] == self.SIXTOP_TYPE_DELETE and pkt['nextTX'] > 1 and self.settings.sixtopInSHARED:
                            pkt['nextTX'] -= 1
                        elif pkt['type'] == self.SIXTOP_TYPE_DELETE_RESPONSE and pkt['nextTX'] > 1 and self.settings.sixtopInSHARED:
                            pkt['nextTX'] -= 1

                # send packet
                if self.pktToSend:
                    if self.pktToSend['type'] == self.SIXTOP_TYPE_ADD or self.pktToSend['type'] == self.SIXTOP_TYPE_RESPONSE:
                        ID = None
                        if self.pktToSend['type'] == self.SIXTOP_TYPE_ADD:
                            ID = self.pktToSend['payload'][3]
                        elif self.pktToSend['type'] == self.SIXTOP_TYPE_RESPONSE or self.pktToSend['type'] == self.SIXTOP_TYPE_DELETE or self.pktToSend['type'] == self.SIXTOP_TYPE_DELETE_RESPONSE:
                            ID = self.pktToSend['payload'][2]
                        # print 'Mote %s is in a shared cell at ASN %s (%d) and sends a packet (transID %s) of type %s (nextTX = %d).' % (str(self.id), str(self.engine.asn), self.engine.getCycle(), ID, str(self.pktToSend['type']), self.pktToSend['nextTX'])
                        self._log(
                            self.INFO,
                            "[tsch] Mote {0} is in a shared cell at ASN {1} ({2}) and sends a packet (transID {3}) of type {4} (nextTX = {5}).",
                            (str(self.id), str(self.engine.asn), self.engine.getCycle(), ID, str(self.pktToSend['type']), self.pktToSend['nextTX']),
                        )
                        # raise -1
                    else:
                        # print 'Mote %s is in a shared cell at ASN %s (%d) and sends a packet of type %s.' % (str(self.id), str(self.engine.asn), self.engine.getCycle(), str(self.pktToSend['type']))
                        self._log(
                            self.INFO,
                            "[tsch] Mote {0} is in a shared cell at ASN {1} ({2}) and sends a packet of type {3}.",
                            (str(self.id), str(self.engine.asn), self.engine.getCycle(), str(self.pktToSend['type'])),
                        )
                    cell['numTx'] += 1

                    self._stats_incrementMoteStats('sharedSlotsTransmits')

                    dmac = None
                    if self.pktToSend['type'] == self.RPL_TYPE_DIO:
                        dmac = self._myNeigbors() # DIO is a broadcast, send to everyone
                    elif self.pktToSend['type'] == self.SIXTOP_TYPE_ADD:
                        # IMPORTANT: the cell is set for all neighbors (myNeighbors()), but the destination should be one neighbor
                        dmac = [self.pktToSend['destination']] # ADD is UNICAST and should be sent to pref parent
                        # print 'On mote %d, the UNICAST address is set to preferred parent %d.' % (self.id, self.pktToSend['destination'].id)
                    elif self.pktToSend['type'] == self.SIXTOP_TYPE_RESPONSE:
                        # IMPORTANT: the cell is set for all neighbors (myNeighbors()), but the destination should be one neighbor
                        dmac = [self.pktToSend['destination']] # RESPONSE is UNICAST and should be sent to child
                        # print 'On mote %d, the UNICAST address is set to preferred parent %d.' % (self.id, self.pktToSend['destination'].id)
                    elif self.pktToSend['type'] == self.SIXTOP_TYPE_DELETE:
                        # IMPORTANT: the cell is set for all neighbors (myNeighbors()), but the destination should be one neighbor
                        dmac = [self.pktToSend['destination']] # DELETE is UNICAST and should be sent to pref parent
                        # print 'On mote %d, the UNICAST address is set to preferred parent %d.' % (self.id, self.pktToSend['destination'].id)
                    elif self.pktToSend['type'] == self.SIXTOP_TYPE_DELETE_RESPONSE:
                        # IMPORTANT: the cell is set for all neighbors (myNeighbors()), but the destination should be one neighbor
                        dmac = [self.pktToSend['destination']] # DELETE RESPONSE is UNICAST and should be sent to child

                    assert dmac != None

                    self.propagation.startTx(
                        channel   = cell['ch'],
                        type      = self.pktToSend['type'],
                        smac      = self,
                        dmac      = dmac, # in a shared slot, this is broadcast
                        payload   = self.pktToSend['payload'],
                    )
                    # indicate that we're waiting for the TX operation to finish
                    self.waitingFor   = self.DIR_TX

                    self._logChargeConsumed(self.CHARGE_TxData_uC)

                else:
                    # start listening
                    self.propagation.startRx(
                         mote          = self,
                         channel       = cell['ch'],
                     )
                    # indicate that we're waiting for the RX operation to finish
                    self.waitingFor = self.DIR_RX

                    # log charge usage
                    self._logChargeConsumed(self.CHARGE_RxData_uC)

            # schedule next active cell
            self._tsch_schedule_activeCell()

    def _tsch_addCells(self,neighbor,cellList,schedule=None,llsfCells=False):
        ''' adds cell(s) to the schedule '''

        with self.dataLock:
            inSchedule = self.schedule # by default
            if self.settings.switchFrameLengths: # it could be that a different schedule is given
                if schedule != None: # if a schedule is given
                    inSchedule = schedule

            for cell in cellList:
                # print cell[0]
                # print inSchedule.keys()
                if llsfCells and cell[0] in inSchedule.keys(): # TODO: in the case it is a llsfCells=True adding, it can be that there is already non-llsfCells in place (this should still be checked somehow)
                    if cell[2] == self.DIR_TX:
                        self.numLLSFCellsToNeighbors[neighbor] -= 1
                    elif cell[2] == self.DIR_RX:
                        self.numLLSFCellsFromNeighbors[neighbor] -= 1
                    continue # in this case, just do not add this one
                if cell[0] in inSchedule.keys():
                    assert inSchedule[cell[0]]['llsfCell'] # if we add a cell and there is already a cell on this spot, it has to be a llsf cell; if it is not something is wrong.
                    # you can just overwrite it, adjust stats
                    # print 'On mote %d' % self.id
                    if inSchedule[cell[0]]['dir'] == self.DIR_TX:
                        # print 'tx'
                        self.numLLSFCellsToNeighbors[inSchedule[cell[0]]['neighbor']] -= 1
                    elif inSchedule[cell[0]]['dir'] == self.DIR_RX:
                        # print 'rx'
                        self.numLLSFCellsFromNeighbors[inSchedule[cell[0]]['neighbor']] -= 1
                    # raise -1
                    del inSchedule[cell[0]]
                inSchedule[cell[0]] = {
                    'ch':                        cell[1],
                    'dir':                       cell[2],
                    'neighbor':                  neighbor,
                    'numTx':                     0,
                    'numTxAck':                  0,
                    'numRx':                     0,
                    'history':                   [],
                    'rxDetectedCollision':       False,
                    'llsfCell':                  llsfCells,
                    'debug_canbeInterfered':     [],                      # [debug] shows schedule collision that can be interfered with minRssi or larger level
                    'debug_interference':        [],                      # [debug] shows an interference packet with minRssi or larger level
                    'debug_lockInterference':    [],                      # [debug] shows locking on the interference packet
                    'debug_cellCreatedAsn':      self.engine.getAsn(),    # [debug]
                }
                # log
                # self._log(
                #     self.INFO,
                #     "[tsch] add cell ts={0} ch={1} dir={2} with {3}",
                #     (cell[0],cell[1],cell[2],neighbor.id if not type(neighbor) == list else self.BROADCAST_ADDRESS),
                # )

            self._log(
                self.INFO,
                '[tsch] On mote {0}, added cells {1} at ASN {2}.',
                (self.id, cellList, self.engine.asn),
            )
            self._tsch_schedule_activeCell()

    def _tsch_blockCells(self,cellList):
        ''' block these cell(s) in the schedule so no sixtop operation can use these cells '''

        with self.dataLock:
            if cellList is None:
                # raise BaseException('seed = %.5f, sf = %s, pkperiod = %s, asn = %s, cycle = %s' % (self.rSeed, str(self.settings.sf), str(self.settings.pkPeriod), str(self.engine.asn), str(self.engine.getCycle())))
                self._log(
                    self.INFO,
                    '[tsch] On mote {0}, during blocking a new cellList, the list was found to be empty.',
                    (str(self.id)),
                )
                cellList = []
            self.blockedCellList += cellList
            self._log(
                self.INFO,
                '[tsch] On mote {0}, block new cells, complete list is now: {1}.',
                (self.id, str(self.blockedCellList)),
            )

    def _tsch_releaseCells(self,cellList):
        ''' release these cell(s) in the schedule so sixtop operations can use these cells again '''

        with self.dataLock:
            self._log(
                self.INFO,
                '[tsch] On mote {0}, about to release following cells: {1}.',
                (self.id, str(cellList)),
            )
            # print 'On mote %d, about to release following cells: %s.' % (self.id, str(cellList))

            listToRemove = []
            cellListTs = [ts for (ts, ch) in cellList]
            # print cellList
            # print 'LIST OF TSs: cellListTs %s -----------------------0000000000000000000000000' % (str(cellListTs))
            for cell in self.blockedCellList:
                if cell[0] in cellListTs:
                    listToRemove.append(cell)

            if len(listToRemove) > 0:
                for cell in listToRemove:
                    self.blockedCellList.remove(cell)

            self._log(
                self.INFO,
                '[tsch] On mote {0}, released cells, complete list is now: {1}.',
                (self.id, str(self.blockedCellList)),
            )
            # print 'On mote %d, released cells, complete list is now: %s.' % (self.id, str(self.blockedCellList))

    def _tsch_removeCells(self,neighbor,tsList,schedule=None):
        ''' removes cell(s) from the schedule '''

        with self.dataLock:
            inSchedule = self.schedule # by default
            if self.settings.switchFrameLengths: # it could be that a different schedule is given
                if schedule != None: # if a schedule is given
                    inSchedule = schedule

            # log
            self._log(
                self.INFO,
                "[tsch] remove timeslots={0} with {1}",
                (tsList,neighbor.id if not type(neighbor) == list else self.BROADCAST_ADDRESS),
            )

            for ts in tsList:
                assert ts in inSchedule.keys()
                assert inSchedule[ts]['neighbor']==neighbor
                inSchedule.pop(ts)

            self._tsch_schedule_activeCell()

    def _tsch_saloha_getNextTXSlot(self):
        return self.randomGen.randint(1, self.TSCH_SALOHA_MAXBACKOFF)

    #===== radio

    def _sixtop_resend_LLSF_reservation_immediately(self, reservationTuple, mote, llsfUniqueID, numCellsOrig):
        ''' Resend the llsf reservation immediately, even delete the sixtopTransactionTX ID. '''
        with self.dataLock:
            old_transID_outgoing = self.sixtopTransactionTX[mote][0]

            self._log(
                self.INFO,
                "[radio] On mote {0}, a LLSF reservation {1} ran out and thus failed. Trying it again.",
                (self.id, reservationTuple),
            )
            # if a LLSF reservation fails, try it again

            # remove the old one, so we can request the new one
            del self.sixtopTransactionTX[mote] # this HAS to happen before the reservation
            transID_outgoing = self._sixtop_cell_llsf_reservation_request(self.preferredParent, reservationTuple, llsfUniqueID=llsfUniqueID, numCellsOrig=numCellsOrig)

            transID_incoming = None
            # look for the tuple with the correct outgoing transID
            toRemoveTuples = []
            for llsf_id, listTuples in self.llsfTransIDs.iteritems():
                for IDsTuple in listTuples:
                    if IDsTuple[1] == old_transID_outgoing and llsf_id == llsfUniqueID:
                        transID_incoming = IDsTuple[0] # the incoming transID
                        toRemoveTuples.append(IDsTuple)
                        break
                    elif IDsTuple[1] == old_transID_outgoing and llsf_id != llsfUniqueID:
                        raise -1
                if transID_incoming != None: # if you already found it, break
                    break

            # remove it
            for tup in toRemoveTuples:
                self.llsfTransIDs[llsfUniqueID].remove(tup)

            ## reservation successful?
            if not transID_outgoing: # no success
                timerValue = 1
                if self.preferredParent in self.sixtopTransactionTX: # NOTE: added this b/c it could be that the it is not the preferred parent anymore by the time you get at this point
                    timerValue = self.sixtopTransactionTX[self.preferredParent][3] + 1
                newASN = self.engine.asn + timerValue
                self.delayedLLSFRequests.append((newASN, transID_incoming, self.preferredParent.id, reservationTuple, llsfUniqueID, numCellsOrig))
            elif transID_outgoing == self.RESERVATION_NOTFEASABLE: # if the reservation is not feasable due to too many cells, delete and forget it
                pass
            else: # yes success
                # add it to transID map
                if llsfUniqueID not in self.llsfTransIDs:
                    self.llsfTransIDs[llsfUniqueID] = []
                # add the new outgoing id
                self.llsfTransIDs[llsfUniqueID].append((transID_incoming, transID_outgoing))
                ######  End - This piece of code makes sure that when a llsf request fails, a new gets send, and the transIDs get replaced ######

    def _sixtop_resend_LLSF_reservation(self, reservationTuple, mote, llsfUniqueID, numCellsOrig, oldOutgoingTransID):
        ''' Resend a recurrent reservation. '''
        with self.dataLock:
            self._log(
                self.INFO,
                "[radio] On mote {0}, a previous recurrent reservation {1} did not end up in a successful reservation. Adding a new one, now with tuple {2}.",
                (self.id, llsfUniqueID, reservationTuple),
            )
            transID_outgoing = self._sixtop_cell_llsf_reservation_request(self.preferredParent, reservationTuple, llsfUniqueID=llsfUniqueID, numCellsOrig=numCellsOrig)

            transID_incoming = None
            # look for the tuple with the correct outgoing transID
            toRemoveTuples = []
            for llsf_id, listTuples in self.llsfTransIDs.iteritems():
                for IDsTuple in listTuples:
                    if IDsTuple[1] == oldOutgoingTransID and llsf_id == llsfUniqueID:
                        transID_incoming = IDsTuple[0] # the incoming transID
                        toRemoveTuples.append(IDsTuple)
                        break
                    elif IDsTuple[1] == oldOutgoingTransID and llsf_id != llsfUniqueID:
                        raise -1
                if transID_incoming != None: # if you already found it, break
                    break

            # remove it
            for tup in toRemoveTuples:
                self.llsfTransIDs[llsfUniqueID].remove(tup)

            ## reservation successful?
            if not transID_outgoing: # no success
                timerValue = self.sixtopTransactionTX[self.preferredParent][3] + 1
                newASN = self.engine.asn + timerValue
                self.delayedLLSFRequests.append((newASN, transID_incoming, self.preferredParent.id, reservationTuple, llsfUniqueID, numCellsOrig))
            elif transID_outgoing == self.RESERVATION_NOTFEASABLE: # if the reservation is not feasable because not suitable reservation could be found, delete and forget it
                pass
            else: # yes success
                # add it to transID map
                if llsfUniqueID not in self.llsfTransIDs:
                    self.llsfTransIDs[llsfUniqueID] = []
                # add the new outgoing id
                self.llsfTransIDs[llsfUniqueID].append((transID_incoming, transID_outgoing))
        ######  End - This piece of code makes sure that when a llsf request fails, a new gets send, and the transIDs get replaced ######

    def findLatestTupleIndex(self, reservationPayload):
        maxASNIndex = None
        maxASN = None
        for index, resTuple in enumerate(reservationPayload):
            if maxASN is None:
                maxASN = resTuple[3] # this is the start ASN
                maxASNIndex = index
            elif resTuple[3] > maxASN:
                maxASN = resTuple[3]
                maxASNIndex = index
        return maxASNIndex

    def radio_txDone(self,isACKed,isNACKed):
        '''end of tx slot'''

        with self.dataLock:
            asn   = self.engine.getAsn()
            ts = None
            if self.settings.switchFrameLengths:
                ts = self.engine.getTimeslot()
            else:
                ts  = asn%self.settings.slotframeLength

            assert ts in self.schedule
            assert self.schedule[ts]['dir']==self.DIR_TX or self.schedule[ts]['dir']==self.DIR_TXRX_SHARED
            assert self.waitingFor==self.DIR_TX

            if isACKed:
                # ACK received

                # update schedule stats
                self.schedule[ts]['numTxAck'] += 1

                # update history
                self.schedule[ts]['history'] += [1]

                # update queue stats
                self._stats_logQueueDelay(asn-self.pktToSend['asn'])

                # time correction
                if self.schedule[ts]['neighbor'] == self.preferredParent:
                    self.timeCorrectedSlot = asn

                if self.pktToSend['type'] == self.SIXTOP_TYPE_ADD:
                    self._stats_incrementMoteStats('zSent6pData')
                    if ((self.settings.sf == 'recurrent' or self.settings.sf == 'recurrent_chang') and self.sixtopTransactionTX[self.pktToSend['destination']][4]):
                        self._stats_incrementMoteStats('zSent6pDataReSF')
                    # print 'On mote %d, received an ACK for a 6P ADD at ASN %d (%d)' % (self.id, self.engine.asn, self.engine.getCycle())
                    self._log(
                        self.INFO,
                        "[radio] On mote {0}, received an ACK for a 6P ADD at ASN {1} ({2})",
                        (self.id, self.engine.asn, self.engine.getCycle()),
                    )
                elif self.pktToSend['type'] == self.SIXTOP_TYPE_RESPONSE:
                    self._stats_incrementMoteStats('zSent6pData')
                    self._log(
                        self.INFO,
                        "[radio] On mote {0}, received an ACK for a 6P RESPONSE at ASN {1} ({2})",
                        (self.id, self.engine.asn, self.engine.getCycle()),
                    )
                    # transID of that transaction
                    transID_incoming = self.sixtopTransactionTXResponses[self.pktToSend['destination']][0]

                    del self.sixtopTransactionTXResponses[self.pktToSend['destination']] # remove the RESPONSE transaction

                    if self.pktToSend['payload'][4] is None: # if it is NOT a recurrent reservation
                        # first release the cells
                        self._tsch_releaseCells(self.pktToSend['payload'][1])
                        # then do the real adding!
                        self._sixtop_action_addCells_RESPONSE(self.pktToSend)
                        self._stats_incrementMoteStats('successful6PTransactionNormal')
                    else: # if it IS a recurrent reservation
                        # block the reservation based on llsfUniqueID and the tuple
                        self._llsf_enable_blocked_reservationsRX(self.pktToSend['payload'][6], self.pktToSend['payload'][4])
                        self._log(
                            self.INFO,
                            "[radio] On mote {0}, got ACK for sent RESPONSE with payload: {1}.",
                            (self.id, self.pktToSend['payload'][4]),
                        )

                        # if received ACK for RESPONSE.
                        # make a reservation to your parent if you are not the DAG root.
                        self._stats_incrementMoteStats('successful6PTransactionLLSF')
                        if not self.dagRoot:
                            timeslot = 0
                            # NOTE: FIXED! (Here you assume that a RESPONSE message for a recurrent reservation only contains 1 reservation formula.
                            # That one is chosen to do the next intermediate reservation.
                            if self.pktToSend['payload'][4] != []: # in this case there was a accepted reservation tuple, it is also possible there was no tuple on which they agreed)
                                self._stats_incrementMoteStats('zSent6pDataReSF')
                                # indexLastTuple = len(self.pktToSend['payload'][4]) - 1 # take the last reservation tuple
                                indexLastTuple = self.findLatestTupleIndex(self.pktToSend['payload'][4])
                                interval = self.pktToSend['payload'][4][indexLastTuple][2]
                                startASN = self.pktToSend['payload'][4][indexLastTuple][3]
                                newStartASN = startASN + 1
                                # print startASN
                                endASN = self.pktToSend['payload'][4][indexLastTuple][4]
                                reservationTuple = (timeslot, interval, newStartASN, endASN) # reassemble the reservation tuple
                                numCellsOrig = self.pktToSend['payload'][5][1]
                                llsfUniqueID = self.pktToSend['payload'][6]

                                # remove outdated reservations
                                # ONLY remove RX reservations because if you leave the TX reservations in here
                                # This way, you can use those cells until the parent agreed on a new reservation with you
                                # see receive_RESPONSE: here you remove the old TX reservation to your parent and the unique IDs
                                if '_c_' in llsfUniqueID: # this means this is an update
                                    prefix = llsfUniqueID.split('_c_')[0]
                                    IDList = self._sixtop_remove_llsf_reservation_with_prefix(prefix, llsfUniqueID, dir='RX')
                                    # uniqueIDList = list(set(IDList))
                                    # for idx in uniqueIDList: # remove the ID(s) in the list
                                    #     print 'removing %s' % idx
                                    #     del self.llsfTransIDs[idx]

                                transID_outgoing = self._sixtop_cell_llsf_reservation_request(self.preferredParent, reservationTuple, llsfUniqueID=llsfUniqueID, numCellsOrig=numCellsOrig)

                                if transID_outgoing: # the reservation request was added successfully
                                    if llsfUniqueID not in self.llsfTransIDs:
                                        self.llsfTransIDs[llsfUniqueID] = []
                                    self.llsfTransIDs[llsfUniqueID].append((transID_incoming, transID_outgoing))
                                elif self.tagTransID == self.RESERVATION_NOTFEASABLE:
                                    pass
                                else:
                                    # add again to delayedLLSFRequests
                                    # the timer value, add one to only do the reservation one after
                                    timerValue = self.sixtopTransactionTX[self.preferredParent][3] + 1
                                    newASN = self.engine.asn + timerValue # try again when this is done.
                                    self.delayedLLSFRequests.append((newASN, transID_incoming, self.preferredParent.id, reservationTuple, llsfUniqueID, numCellsOrig))

                                self._stats_incrementMoteStats('successful6PTransactionLLSFIntermediate')
                        else:
                            llsfUniqueID = self.pktToSend['payload'][6]
                            if self.pktToSend['payload'][4] != []:
                                self._stats_incrementMoteStats('zSent6pDataReSF')
                                # print 'Received a successful (unique ID: %s) 6P on root.' % llsfUniqueID
                                self._log(
                                    self.INFO,
                                    "[radio] Received a successful (unique ID: %s) 6P on root.",
                                    (llsfUniqueID),
                                )

                                # print self.llsfTransIDs
                                # remove outdated reservations
                                if '_c_' in llsfUniqueID: # this means this is an update
                                    prefix = llsfUniqueID.split('_c_')[0]
                                    IDList = self._sixtop_remove_llsf_reservation_with_prefix(prefix, llsfUniqueID, dir='RX')
                                    # apperentaly we doe not save this on the root?
                                    # uniqueIDList = list(set(IDList))
                                    # for idx in uniqueIDList: # remove the ID(s) in the list
                                    #     print 'removing %s' % idx
                                    #     del self.llsfTransIDs[idx]

                                self.engine.successLLSFMotes.append(llsfUniqueID)
                                self._stats_incrementMoteStats('successful6PTransactionLLSFRoot')
                            else:
                                self._log(
                                    self.INFO,
                                    "[radio] Received a recurrent reservation (unique ID: {0}), but we did not agree on anything.",
                                    (llsfUniqueID),
                                )
                                # print 'Received a recurrent reservation (unique ID: %s), but we did not agree on anything.' % llsfUniqueID

                if self.pktToSend['type'] == self.SIXTOP_TYPE_DELETE:
                    self._stats_incrementMoteStats('zSent6pData')
                    # print 'On mote %d, received an ACK for a 6P DELETE at ASN %d (%d)' % (self.id, self.engine.asn, self.engine.getCycle())
                    self._log(
                        self.INFO,
                        "[radio] On mote {0}, received an ACK for a 6P DELETE at ASN {1} ({2}).",
                        (self.id, self.engine.asn, self.engine.getCycle()),
                    )
                elif self.pktToSend['type'] == self.SIXTOP_TYPE_DELETE_RESPONSE:
                    self._stats_incrementMoteStats('zSent6pData')

                    # remove the RESPONSE deletion
                    del self.sixtopDeletionTXResponses[self.pktToSend['destination']]

                    # print 'On mote %d, received an ACK for a 6P DELETE RESPONSE at ASN %d (%d)' % (self.id, self.engine.asn, self.engine.getCycle())
                    self._log(
                        self.INFO,
                        "[radio] On mote {0}, received an ACK for a 6P DELETE RESPONSE at ASN {1} ({2}).",
                        (self.id, self.engine.asn, self.engine.getCycle()),
                    )

                    # do the deleting
                    self._sixtop_action_removeCells_DELETE_RESPONSE(self.pktToSend)

                # remove packet from queue
                self.txQueue.remove(self.pktToSend)

            elif isNACKed:
                # NACK received

                # update schedule stats as if it were successfully transmitted
                self.schedule[ts]['numTxAck'] += 1

                # update history
                self.schedule[ts]['history'] += [1]

                # time correction
                if self.schedule[ts]['neighbor'] == self.preferredParent:
                    self.timeCorrectedSlot = asn

                # NOTE: maybe I am misusing the definition of a NACK here, but it works.
                # it can be that the RESPONSEs are NACK'd! DO NOT do their resp. actions in this case.
                if self.pktToSend['type'] == self.SIXTOP_TYPE_RESPONSE:
                    self._stats_incrementMoteStats('zSent6pData')
                    # print 'On mote %d, received an NACK for a 6P RESPONSE at ASN %d (%d): not adding cells.' % (self.id, self.engine.asn, self.engine.getCycle())
                    self._log(
                        self.INFO,
                        "[radio] On mote {0}, received an NACK for a 6P RESPONSE at ASN {1} ({2}): not adding cells..",
                        (self.id, self.engine.asn, self.engine.getCycle()),
                    )
                    # remove the RESPONSE transaction
                    del self.sixtopTransactionTXResponses[self.pktToSend['destination']]

                    if self.pktToSend['payload'][4] is None:
                        # do not do the adding
                        # self.pktToSend['payload'][1] are the cells
                        self._tsch_releaseCells(self.pktToSend['payload'][1])
                        # remove packet from queue
                        self.txQueue.remove(self.pktToSend)
                    else:
                        self._stats_incrementMoteStats('zSent6pDataReSF')
                        # do not do the adding
                        # remove packet from queue
                        self.txQueue.remove(self.pktToSend)
                        # remove the blocked reservations when a NACK happens, based on the transID
                        self._llsf_remove_blocked_reservationsRX(self.pktToSend['payload'][6])
                elif self.pktToSend['type'] == self.SIXTOP_TYPE_DELETE_RESPONSE:
                    self._stats_incrementMoteStats('zSent6pData')
                    # print 'On mote {0}, received an NACK for a 6P DELETE RESPONSE at ASN {1} ({2}): not deleting cells.' % (self.id, self.engine.asn, self.engine.getCycle())
                    self._log(
                        self.INFO,
                        "[radio] On mote {0}, received an NACK for a 6P DELETE RESPONSE at ASN {1} ({2}): not deleting cells.",
                        (self.id, self.engine.asn, self.engine.getCycle()),
                    )
                    # remove the RESPONSE deletion
                    del self.sixtopDeletionTXResponses[self.pktToSend['destination']]

                    # remove packet from queue
                    self.txQueue.remove(self.pktToSend)
                    # raise -1
                else: # with all other packets receiving a NACK means a failed transmission and one should decrement the retry counter

                    if self.pktToSend['type'] == self.SIXTOP_TYPE_DELETE:
                        self._stats_incrementMoteStats('zSent6pData')
                    if self.pktToSend['type'] == self.SIXTOP_TYPE_ADD:
                        self._stats_incrementMoteStats('zSent6pData')
                        if ((self.settings.sf == 'recurrent' or self.settings.sf == 'recurrent_chang') and self.sixtopTransactionTX[self.pktToSend['destination']][4]):
                            self._stats_incrementMoteStats('zSent6pDataReSF')

                    # decrement 'retriesLeft' counter associated with that packet
                    i = self.txQueue.index(self.pktToSend)
                    if self.txQueue[i]['retriesLeft'] > 0:
                        self.txQueue[i]['retriesLeft'] -= 1

                    # drop packet if retried too many times
                    if self.txQueue[i]['retriesLeft'] == 0:

                        if len(self.txQueue) == self.TSCH_QUEUE_SIZE:
                            if self.pktToSend['type'] == self.APP_TYPE_DATA: # update mote stats
                                self._stats_incrementMoteStats('droppedMacRetries')
                            else:
                                self._stats_incrementMoteStats('droppedMacRetriesTop')

                            # remove the transaction when it is an add or delete
                            if self.pktToSend['type'] == self.SIXTOP_TYPE_ADD and self.pktToSend['payload'][3] == self.sixtopTransactionTX[self.pktToSend['destination']][0]:
                                # print 'At mote %d, removed sixtop transaction because it ran out at retries (last time was a NACK) at %d.' % (self.id, self.engine.asn)
                                self._log(
                                    self.INFO,
                                    "[radio] At mote {0}, removed sixtop transaction because it ran out at retries (last time was a NACK) at {1}.",
                                    (self.id, self.engine.asn),
                                )
                                # also remove the blocked LLSF cells because the timer ran out, based on the transID
                                # only if it is a llsf transaction (indicated by value at index 4)
                                if (self.settings.sf == 'recurrent' or  self.settings.sf == 'recurrent_chang') and self.sixtopTransactionTX[self.pktToSend['destination']][4]:
                                    self._llsf_remove_blocked_reservationsTX(self.pktToSend['payload'][6])
                                else:
                                    self._tsch_releaseCells(self.sixtopTransactionTX[self.pktToSend['destination']][1])

                                del self.sixtopTransactionTX[self.pktToSend['destination']]
                            elif self.pktToSend['type'] == self.SIXTOP_TYPE_DELETE and self.pktToSend['payload'][2] == self.sixtopDeletionTX[self.pktToSend['destination']][0]:
                                self._log(
                                    self.INFO,
                                    "[radio] At mote {0}, removed sixtop deletion (deleteID {1}) because it ran out at retries (last time was a NACK) at ASN {2}.",
                                    (self.id, self.sixtopDeletionTX[self.pktToSend['destination']][0], self.engine.asn),
                                )
                                del self.sixtopDeletionTX[self.pktToSend['destination']]

                            # remove packet from queue
                            self.txQueue.remove(self.pktToSend)

            elif self.pktToSend['type'] == self.RPL_TYPE_DIO:
                # broadcast packet is not acked, remove from queue and update stats
                self.txQueue.remove(self.pktToSend)
            else:
                # neither ACK nor NACK received
                i = self.txQueue.index(self.pktToSend)

                if (self.settings.sixtopInSHARED and self.settings.sixtopInTXRX) and (self.pktToSend['type'] == self.SIXTOP_TYPE_ADD or self.pktToSend['type'] == self.SIXTOP_TYPE_RESPONSE or self.pktToSend['type'] == self.SIXTOP_TYPE_DELETE or self.pktToSend['type'] == self.SIXTOP_TYPE_DELETE_RESPONSE):
                    nextSALOHASlot = self.randomGen.randint(1, self.TSCH_SALOHA_MAXBACKOFF)
                    self.txQueue[i]['nextTX'] = nextSALOHASlot
                    ID = None
                    if self.pktToSend['type'] == self.SIXTOP_TYPE_ADD:
                        ID = self.pktToSend['payload'][3]
                        self._stats_incrementMoteStats('zSent6pData')
                        if (self.settings.sf == 'recurrent' or  self.settings.sf == 'recurrent_chang') and self.sixtopTransactionTX[self.pktToSend['destination']][4]:
                            self._stats_incrementMoteStats('zSent6pDataReSF')
                    elif self.pktToSend['type'] == self.SIXTOP_TYPE_RESPONSE or self.pktToSend['type'] == self.SIXTOP_TYPE_DELETE or self.pktToSend['type'] == self.SIXTOP_TYPE_DELETE_RESPONSE:
                        ID = self.pktToSend['payload'][2]
                        self._stats_incrementMoteStats('zSent6pData')
                        if self.pktToSend['type'] == self.SIXTOP_TYPE_RESPONSE and ((self.settings.sf == 'recurrent' or  self.settings.sf == 'recurrent_chang') and self.sixtopTransactionTXResponses[self.pktToSend['destination']][4]):
                            self._stats_incrementMoteStats('zSent6pDataReSF')
                    self._log(
                        self.INFO,
                        "[radio] On mote {0}, after TX did not receive ACK/NACK for a 6P {1} (with ID {2}) at ASN {3} ({4}): next attempt will be in {5} SHARED slots (or in next dedicated slot).",
                        (self.id, self.pktToSend['type'], ID, self.engine.asn, self.engine.getCycle(), nextSALOHASlot),
                    )
                    self._log(
                        self.INFO,
                        "[radio] On mote {0}, llsfUnique ID = .",
                        (self.id, self.pktToSend['type'], ID, self.engine.asn, self.engine.getCycle(), nextSALOHASlot),
                    )

                # update history
                self.schedule[ts]['history'] += [0]

                # decrement 'retriesLeft' counter associated with that packet
                if self.txQueue[i]['retriesLeft'] > 0:
                    self.txQueue[i]['retriesLeft'] -= 1

                # drop packet if retried too many times
                if self.txQueue[i]['retriesLeft'] == 0:

                    if  len(self.txQueue) == self.TSCH_QUEUE_SIZE:

                        # update mote stats
                        if self.pktToSend['type'] == self.APP_TYPE_DATA:
                            # update mote stats
                            self._stats_incrementMoteStats('droppedMacRetries')
                        else:
                            self._stats_incrementMoteStats('droppedMacRetriesTop')

                        # remove the transaction when it is an add or delete
                        if self.pktToSend['type'] == self.SIXTOP_TYPE_ADD and self.pktToSend['payload'][3] == self.sixtopTransactionTX[self.pktToSend['destination']][0]:
                            self._log(
                                self.INFO,
                                "[radio] At mote {0}, removed sixtop transaction because it ran out at retries at {1}.",
                                (self.id, self.engine.asn),
                            )
                            # also remove the blocked LLSF cells because the timer ran out, based on the transID
                            # only if it is a llsf transaction (indicated by value at index 4)
                            if (self.settings.sf == 'recurrent' or  self.settings.sf == 'recurrent_chang') and self.sixtopTransactionTX[self.pktToSend['destination']][4]:
                                self._llsf_remove_blocked_reservationsTX(self.pktToSend['payload'][6])

                                ######  Begin - This piece of code makes sure that when a llsf request fails, a new gets send, and the transIDs get replaced ######

                                # get the information for the new try
                                timeslot = self.sixtopTransactionTX[self.pktToSend['destination']][5][0]
                                interval = self.sixtopTransactionTX[self.pktToSend['destination']][5][1]
                                startASN = self.sixtopTransactionTX[self.pktToSend['destination']][5][2]
                                endASN = self.sixtopTransactionTX[self.pktToSend['destination']][5][3]
                                numCellsOrig = self.sixtopTransactionTX[self.pktToSend['destination']][7] # original number of cells
                                reservationTuple = (timeslot, interval, startASN, endASN)

                                self._sixtop_resend_LLSF_reservation_immediately(reservationTuple, self.pktToSend['destination'], self.pktToSend['payload'][6], numCellsOrig)
                            else:
                                self._tsch_releaseCells(self.sixtopTransactionTX[self.pktToSend['destination']][1])
                                # remove the transaction because you are out of retries
                                del self.sixtopTransactionTX[self.pktToSend['destination']]

                        elif self.pktToSend['type'] == self.SIXTOP_TYPE_DELETE and self.pktToSend['payload'][2] == self.sixtopDeletionTX[self.pktToSend['destination']][0]:
                            # print 'At mote %d, removed sixtop deletion (deleteID %s) because it ran out at retries at ASN %d.' % (self.id, self.sixtopDeletionTX[self.pktToSend['destination']][0], self.engine.asn)
                            self._log(
                                self.INFO,
                                "[radio] At mote {0}, removed sixtop deletion (deleteID {1}) because it ran out at retries at ASN {2}.",
                                (self.id, self.sixtopDeletionTX[self.pktToSend['destination']][0], self.engine.asn),
                            )
                            # remove the deletion because you are out of retries
                            del self.sixtopDeletionTX[self.pktToSend['destination']]
                        elif self.pktToSend['type'] == self.SIXTOP_TYPE_RESPONSE and self.pktToSend['payload'][2] == self.sixtopTransactionTXResponses[self.pktToSend['destination']][0]:
                            # print 'At mote %d, removed sixtop RESPONSE transaction because it ran out at retries at %d.' % (self.id, self.engine.asn)
                            self._log(
                                self.INFO,
                                "[radio] At mote {0}, removed sixtop RESPONSE transaction because it ran out at retries at {1}.",
                                (self.id, self.engine.asn),
                            )
                            # also remove the blocked LLSF cells because the timer ran out, based on the transID
                            # only if it is a llsf transaction (indicated by value at index 4)
                            if (self.settings.sf == 'recurrent' or  self.settings.sf == 'recurrent_chang') and self.sixtopTransactionTXResponses[self.pktToSend['destination']][4]:
                                self._llsf_remove_blocked_reservationsRX(self.pktToSend['payload'][6])
                            else:
                                self._tsch_releaseCells(self.sixtopTransactionTXResponses[self.pktToSend['destination']][1])

                            # remove the RESPONSE transaction, because you are out of retries
                            del self.sixtopTransactionTXResponses[self.pktToSend['destination']]

                        elif self.pktToSend['type'] == self.SIXTOP_TYPE_DELETE_RESPONSE and self.pktToSend['payload'][2] == self.sixtopDeletionTXResponses[self.pktToSend['destination']][0]:
                            # print 'At mote %d, removed sixtop RESPONSE deletion (deleteID %s) because it ran out at retries at ASN %d.' % (self.id, self.sixtopDeletionTXResponses[self.pktToSend['destination']][0], self.engine.asn)
                            self._log(
                                self.INFO,
                                "[radio] At mote {0}, removed sixtop RESPONSE deletion (deleteID {1}) because it ran out at retries at ASN {2}.",
                                (self.id, self.sixtopDeletionTXResponses[self.pktToSend['destination']][0], self.engine.asn),
                            )
                            # remove the RESPONSE deletion, because you are out of retries
                            del self.sixtopDeletionTXResponses[self.pktToSend['destination']]

                        # remove packet from queue
                        self.txQueue.remove(self.pktToSend)

            # end of radio activity, not waiting for anything
            self.waitingFor = None

            # for debug
            ch = self.schedule[ts]['ch']
            rx = self.schedule[ts]['neighbor']
            llsfcelll = self.schedule[ts]['llsfCell']
            canbeInterfered = 0
            for mote in self.engine.motes:
                if mote == self:
                    continue
                if ts in mote.schedule and ch == mote.schedule[ts]['ch'] and mote.schedule[ts]['dir'] == self.DIR_TX:
                    if mote.id == rx.id or mote.getRSSI(rx)>rx.minRssi:
                        canbeInterfered = 1
            self.schedule[ts]['debug_canbeInterfered'] += [canbeInterfered]

    def radio_rxDone(self,type=None,smac=None,dmac=None,payload=None):
        '''end of RX radio activity'''

        with self.dataLock:

            asn   = self.engine.getAsn()
            ts = None
            if self.settings.switchFrameLengths:
                ts = self.engine.getTimeslot()
            else:
                ts  = asn%self.settings.slotframeLength

            assert ts in self.schedule
            assert self.schedule[ts]['dir']==self.DIR_RX or self.schedule[ts]['dir']==self.DIR_TXRX_SHARED
            assert self.waitingFor==self.DIR_RX

            if smac:
                # I received a packet

                # log charge usage
                self._logChargeConsumed(self.CHARGE_RxDataTxAck_uC)

                # update schedule stats
                self.schedule[ts]['numRx'] += 1
                if (type == self.RPL_TYPE_DIO):
                    # got a DIO
                    self._rpl_action_receiveDIO(type, smac, payload)
                    (isACKed, isNACKed) = (False, False)
                    self.waitingFor = None

                    return isACKed, isNACKed

                elif type == self.SIXTOP_TYPE_ADD or type == self.SIXTOP_TYPE_DELETE:
                    # got a 6P ADD message
                    self._log(
                        self.INFO,
                        '[radio] On mote {0}, received a {1} at ASN {2} ({3})',
                        (self.id, type, self.engine.asn, self.engine.getCycle()),
                    )
                    # do 6P stuff
                    if type == self.SIXTOP_TYPE_ADD:
                        self._sixtop_action_receive_ADD(type, smac, payload)
                    elif type == self.SIXTOP_TYPE_DELETE:
                        self._sixtop_action_receive_DELETE(type, smac, payload)

                    (isACKed, isNACKed) = (True, False)

                    self.waitingFor = None

                    return isACKed, isNACKed
                elif type == self.SIXTOP_TYPE_RESPONSE or type == self.SIXTOP_TYPE_DELETE_RESPONSE:
                    # got a 6P ADD message
                    self._log(
                        self.INFO,
                        '[radio] On mote {0}, received a {1} at ASN {2} ({3})',
                        (self.id, type, self.engine.asn, self.engine.getCycle()),
                    )

                    ack = False
                    # do 6P stuff
                    if type == self.SIXTOP_TYPE_RESPONSE:
                        ack = self._sixtop_action_receive_RESPONSE(type, smac, payload)
                    elif type == self.SIXTOP_TYPE_DELETE_RESPONSE:
                        ack = self._sixtop_action_receive_DELETE_RESPONSE(type, smac, payload)

                    if ack:
                        (isACKed, isNACKed) = (True, False)
                        if type == self.SIXTOP_TYPE_RESPONSE:
                            del self.sixtopTransactionTX[smac] # remove the transaction, because it finished succesfully
                        elif type == self.SIXTOP_TYPE_DELETE_RESPONSE:
                            del self.sixtopDeletionTX[smac] # remove the deletion, because it finished succesfully
                    else:
                        (isACKed, isNACKed) = (False, True)

                    self.waitingFor = None

                    return isACKed, isNACKed

                    # todo account for stats.

                if self.dagRoot and type == self.APP_TYPE_DATA:
                    # receiving packet (at DAG root)
                    self._log(
                        self.INFO,
                        '[radio] On dagRoot at ASN {0} ({1}), I received a packet.',
                        (self.engine.asn, self.engine.getCycle()),
                    )
                    # print 'On dagRoot at ASN %s (%s), I received a packet.' % (str(self.engine.asn), str(self.engine.getCycle()))

                    # update mote stats
                    self._stats_incrementMoteStats('appReachesDagroot')

                    # log it to the engine
                    # payload[0] is the source mote
                    if payload[0] not in self.engine.delayPerSource:
                        self.engine.delayPerSource[payload[0]] = []
                    self.engine.delayPerSource[payload[0]].append(asn-payload[1])

                    # calculate end-to-end latency
                    self._stats_logLatencyStat(asn-payload[1])

                    # log the number of hops
                    self._stats_logHopsStat(payload[2])

                    (isACKed, isNACKed) = (True, False)

                else:
                    # relaying packet

                    # count incoming traffic for each node if it is not an already registered llsf packet!
                    incoming_transID = payload[3]
                    llsfUniqueID = payload[4]

                    # do not tag the outgoing packet by default
                    tagIt = False
                    # check first if there is a reservation tuple with this transID
                    for resTuple in self.LLSFReservationsRXTuples:
                        if incoming_transID == resTuple[5]:
                            reservationASNs = self._llsf_get_reservationASNs(resTuple[3], resTuple[4], resTuple[2])
                            if self.engine.asn in reservationASNs:
                                tagIt = True # a correctly tagged packet was received at a correct ASN
                                break

                    # then check for the outgoing transID
                    outgoing_transID = None
                    if tagIt and llsfUniqueID in self.llsfTransIDs:
                        list_IDTuples = self.llsfTransIDs[llsfUniqueID]
                        for IDTuple in list_IDTuples:
                            if IDTuple[0] == incoming_transID:
                                outgoing_transID = IDTuple[1]
                                break

                    # if outgoing_transID == None: # if it is None, log it, because this means this was a packet that was not supposed to be here.
                    if self.settings.sf != 'recurrent' and self.settings.sf != 'recurrent_chang':
                        self._otf_incrementIncomingTraffic(smac)
                    # else:
                    #     self._otf_incrementIncomingTrafficLLSF(smac)

                    self._log(
                        self.INFO,
                        '[radio] On mote {0} at ASN {1} (cycle: {2}, ts: {3}), I\'m relaying a packet.',
                        (self.id, self.engine.asn, self.engine.getCycle(), ts),
                    )
                    # print 'On mote %s at ASN %s (cycle: %s, ts: %d), I\'m relaying a packet.' % (str(self.id), str(self.engine.asn), str(self.engine.getCycle()), ts)

                    # update the number of hops
                    newPayload     = copy.deepcopy(payload)
                    newPayload[2] += 1
                    newPayload[3] = outgoing_transID # can be None, can be the good outgoing_transID: depends on whether this mote has a reservation

                    # create packet
                    relayPacket = {
                        'asn':         asn,
                        'type':        type,
                        'payload':     newPayload,
                        'retriesLeft': self.TSCH_MAXTXRETRIES
                    }

                    # enqueue packet in TSCH queue
                    isEnqueued = self._tsch_enqueue_relayed(relayPacket)

                    if isEnqueued:
                        # update mote stats
                        self._stats_incrementMoteStats('appRelayed')
                        (isACKed, isNACKed) = (True, False)
                    else:
                        (isACKed, isNACKed) = (False, True)

            else:
                # self._log(
                #     self.INFO,
                #     '[radio] On mote {0} at ASN {1} ({2}), I did an IDLE listen.',
                #     (str(self.id), str(self.engine.asn), str(self.engine.getCycle())),
                # )

                # log charge usage
                self._logChargeConsumed(self.CHARGE_Idle_uC)

                (isACKed, isNACKed) = (False, False)

            self.waitingFor = None

            return isACKed, isNACKed

    #===== wireless

    def setPDR(self,neighbor,pdr):
        ''' sets the pdr to that neighbor'''
        with self.dataLock:
            self.PDR[neighbor] = pdr

    def getPDR(self,neighbor):
        ''' returns the pdr to that neighbor'''
        with self.dataLock:
            return self.PDR[neighbor]

    def setRSSI(self,neighbor,rssi):
        ''' sets the RSSI to that neighbor'''
        with self.dataLock:
            self.RSSI[neighbor.id] = rssi

    def getRSSI(self,neighbor):
        ''' returns the RSSI to that neighbor'''
        with self.dataLock:
            # print neighbor.id
            # print self.id
            # print self.RSSI
            return self.RSSI[neighbor.id]

    def _estimateETX(self,neighbor):

        with self.dataLock:

            # set initial values for numTx and numTxAck assuming PDR is exactly estimated
            pdr                   = self.getPDR(neighbor)
            numTx                 = self.NUM_SUFFICIENT_TX
            numTxAck              = math.floor(pdr*numTx)

            for (_,cell) in self.schedule.items():
                if (cell['neighbor'] == neighbor) and (cell['dir'] == self.DIR_TX):
                    numTx        += cell['numTx']
                    numTxAck     += cell['numTxAck']

            # abort if about to divide by 0r
            if not numTxAck:
                return

            # calculate ETX
            etx = float(numTx)/float(numTxAck)

            return etx

    def _myNeigbors(self):
        return [n for n in self.PDR.keys() if self.PDR[n]>0]

    #===== clock

    def clock_getOffsetToDagRoot(self):
        ''' calculate time offset compared to the DAGroot '''

        if self.dagRoot:
            return 0.0

        asn                  = self.engine.getAsn()
        offset               = 0.0
        child                = self
        parent               = self.preferredParent

        while True:
            secSinceSync     = (asn-child.timeCorrectedSlot)*self.settings.slotDuration  # sec
            # FIXME: for ppm, should we not /10^6?
            relDrift         = child.drift - parent.drift                                # ppm
            offset          += relDrift * secSinceSync                                   # us
            if parent.dagRoot:
                break
            else:
                if child.preferredParent.preferredParent == child:
                    print 'This mote id: %d' % self.id
                    print 'Child: %d' % child.id
                    print 'Child pref parent: %d' % child.preferredParent.id
                    print 'Child pref parent pref parent: %d' % child.preferredParent.preferredParent.id
                    # raise BaseException('asn = %d, seed = %.5f, sf = %s, pkperiod = %s' % (self.engine.asn, self.rSeed, str(self.settings.sf), str(self.settings.pkPeriod)))
                    # raise -1
                    raise -1
                child        = parent
                parent       = child.preferredParent

        return offset

    #===== location

    def setLocation(self,x,y):
        with self.dataLock:
            self.x = x
            self.y = y

    def getLocation(self):
        with self.dataLock:
            return (self.x,self.y)

    #==== battery

    def boot(self):
        # start the stack layer by layer
        #
        print '\r\n'
        print 'Booting mote %d.' % self.id
        # print 'Sixtop timeout init: %.4f.' % self.sixtop_timout_init
        assert self.sixtop_timout_init >= 101
        print 'OTF housekeeping delay of mote %d is %.3f' % (self.id, self.bootDelay)

        if self.settings.switchFrameLengths:
            self.schedule = self.mgmtSchedule

        # add minimal cell
        # if self.settings.switchFrameLengths:
        #     # fill the default frame with one shared cell (minimal configuration)
        #     self._tsch_addCells(self._myNeigbors(),[(0,0,self.DIR_TXRX_SHARED)], self.defaultSchedule)
        #     # fill the mgmt frame with shared cells
        #     self._tsch_addCells(self._myNeigbors(),[(0,0,self.DIR_TXRX_SHARED)], self.mgmtSchedule)
        #     self._tsch_addCells(self._myNeigbors(),[(1,1,self.DIR_TXRX_SHARED)], self.mgmtSchedule)
        #     self._tsch_addCells(self._myNeigbors(),[(2,2,self.DIR_TXRX_SHARED)], self.mgmtSchedule)
        #     self._tsch_addCells(self._myNeigbors(),[(3,3,self.DIR_TXRX_SHARED)], self.mgmtSchedule)
        #     self._tsch_addCells(self._myNeigbors(),[(4,4,self.DIR_TXRX_SHARED)], self.mgmtSchedule)
        #     self._tsch_addCells(self._myNeigbors(),[(5,5,self.DIR_TXRX_SHARED)], self.mgmtSchedule)
        #     self._tsch_addCells(self._myNeigbors(),[(6,6,self.DIR_TXRX_SHARED)], self.mgmtSchedule)
        #     self._tsch_addCells(self._myNeigbors(),[(7,7,self.DIR_TXRX_SHARED)], self.mgmtSchedule)
        #     self._tsch_addCells(self._myNeigbors(),[(8,8,self.DIR_TXRX_SHARED)], self.mgmtSchedule)
        #     self._tsch_addCells(self._myNeigbors(),[(9,9,self.DIR_TXRX_SHARED)], self.mgmtSchedule)
        # else:
        self._tsch_addCells(self._myNeigbors(),[(0,0,self.DIR_TXRX_SHARED)])
        self._tsch_addCells(self._myNeigbors(),[(1,1,self.DIR_TXRX_SHARED)])
        self._tsch_addCells(self._myNeigbors(),[(2,2,self.DIR_TXRX_SHARED)])
        # RPL
        self._rpl_schedule_sendDIO(firstDIO=True)
        # setting ETX
        self._schedule_set_ETX()
        # OTF
        self._otf_resetInboundTrafficCounters()
        self._otf_schedule_housekeeping(True)
        # 6top
        if not self.settings.sixtopNoHousekeeping:
            self._sixtop_schedule_housekeeping()

        # tsch
        self._tsch_schedule_activeCell()

        # app
        if not self.dagRoot:
            # if self.minimalASN != None:
            if self.id in self.engine.getFirstDelays():
                if self.settings.sf == 'recurrent_chang' or self.settings.sf == 'recurrent':
                    self._app_schedule_recurrentReservation()
                if self.settings.dynamicNetwork:
                    self._app_schedule_changingRecurrentReservation()
                    self._app_schedule_sendSinglePacket_CF() # special scheduling of packets funtions for a network where the frequencies are dynamic
                else:
                    self._app_schedule_sendSinglePacket()

    def _logChargeConsumed(self,charge):
        with self.dataLock:
            self.chargeConsumed  += charge

    #======================== private =========================================

    #===== getters

    def getTxCells(self):
        with self.dataLock:
            return [(ts,c['ch'],c['neighbor']) for (ts,c) in self.schedule.items() if c['dir']==self.DIR_TX]

    def getTxCellsRecurrent(self):
        with self.dataLock:
            return [(ts,c['ch'],c['neighbor']) for (ts,c) in self.schedule.items() if c['dir']==self.DIR_TX and c['llsfCell']]

    # meant for downwards traffic, do I have a cell to that particalur child-neighbor?
    def getTxCellsNeighbor(self, neighbor):
        with self.dataLock:
            return [(ts,c['ch'],c['neighbor']) for (ts,c) in self.schedule.items() if (c['dir']==self.DIR_TX and c['neighbor'] == neighbor)]

    # do I have a cell to that particalur neighbor?
    def getTxCellsNeighborNoRecurrent(self, neighbor):
        with self.dataLock:
            return [(ts,c['ch'],c['neighbor']) for (ts,c) in self.schedule.items() if (c['dir']==self.DIR_TX and c['neighbor'] == neighbor and not c['llsfCell'])]

    def getRxCells(self):
        with self.dataLock:
            return [(ts,c['ch'],c['neighbor']) for (ts,c) in self.schedule.items() if c['dir']==self.DIR_RX]

    def getRxCellsRecurrent(self):
        with self.dataLock:
            return [(ts,c['ch'],c['neighbor']) for (ts,c) in self.schedule.items() if c['dir']==self.DIR_RX and c['llsfCell']]

    def getSharedCells(self):
        with self.dataLock:
            return [(ts, c['ch'], c['neighbor']) for (ts, c) in self.schedule.items() if c['dir'] == self.DIR_TXRX_SHARED]

    #===== stats

    # mote state

    def getMoteStats(self):

        # gather statistics
        with self.dataLock:
            returnVal = copy.deepcopy(self.motestats)
            returnVal['numTxCells']         = len(self.getTxCells())
            returnVal['numRxCells']         = len(self.getRxCells())
            returnVal['numTxCellsRecurrent']= len(self.getTxCellsRecurrent())
            returnVal['numRxCellsRecurrent']= len(self.getRxCellsRecurrent())
            returnVal['numSharedCells']     = len(self.getSharedCells())
            returnVal['aveQueueDelay']      = self._stats_getAveQueueDelay()
            returnVal['aveQueueDelayStd']   = self._stats_getAveQueueDelayStd()
            returnVal['aveLatency']         = self._stats_getAveLatency()
            returnVal['aveLatencyStd']      = self._stats_getAveLatencyStd()
            returnVal['aveETX']             = self._stats_getAveETX()
            returnVal['aveETXStd']          = self._stats_getAveETXStd()
            returnVal['aveHops']            = self._stats_getAveHops()
            returnVal['aveHopsStd']         = self._stats_getAveHopsStd()
            returnVal['probableCollisions'] = self._stats_getRadioStats('probableCollisions')
            returnVal['txQueueFill']        = len(self.txQueue)
            returnVal['chargeConsumed']     = self.chargeConsumed
            returnVal['numTx']              = sum([cell['numTx'] for (_,cell) in self.schedule.items()])
            returnVal['txQueueFillData']    = None

            lenQueueData = 0
            for pkt in self.txQueue:
                if pkt['type'] == self.APP_TYPE_DATA:
                    lenQueueData += 1

            returnVal['txQueueFillData'] = lenQueueData

        # reset the statistics
        self._stats_resetMoteStats()
        self._stats_resetQueueStats()
        self._stats_resetLatencyStats()
        self._stats_resetHopsStats()
        self._stats_resetRadioStats()
        self._stats_resetETXStats()

        return returnVal

    def _stats_resetMoteStats(self):
        with self.dataLock:
            self.motestats = {
                # app
                'appGenerated':            0,   # number of packets app layer generated
                'appRelayed':              0,   # number of packets relayed
                'appReachesDagroot':       0,   # number of packets received at the DAGroot
                'droppedAppFailedEnqueue': 0,   # dropped packets because app failed enqueue them
                'notDroppedNoTxCells':          0,   #
                'notDroppedNoTxCellsRelayed':   0,   #
                'notDroppedNoTxCellsTop':       0,   #
                # queue
                'droppedQueueFull':        0,   # dropped packets because queue is full
                'droppedQueueFullRelayed':        0,   # dropped packets because queue is full
                'droppedQueueFullTop':        0,   # dropped packets because queue is full
                # rpl
                'rplTxDIO':                0,   # number of TX'ed DIOs
                'rplRxDIO':                0,   # number of RX'ed DIOs
                'rplChurnPrefParent':      0,   # number of time the mote changes preferred parent
                'rplChurnRank':            0,   # number of time the mote changes rank
                'rplChurnParentSet':       0,   # number of time the mote changes parent set
                'droppedNoRoute':          0,   # packets dropped because no route (no preferred parent)
                'droppedNoRouteRelayed':          0,   # packets dropped because no route (no preferred parent)
                'droppedNoRouteTop':          0,   # packets dropped because no route (no preferred parent)
                # otf
                'otfAdd':                  0,   # OTF adds some cells
                'otfRemove':               0,   # OTF removes some cells
                'droppedNoTxCells':        0,   # packets dropped because no TX cells
                'droppedNoTxCellsRelayed':        0,   # packets dropped because no TX cells
                'droppedNoTxCellsTop':        0,   # packets dropped because no TX cells
                # 6top
                'topTxRelocatedCells':     0,   # number of time tx-triggered 6top relocates a single cell
                'topTxRelocatedBundles':   0,   # number of time tx-triggered 6top relocates a bundle
                'topRxRelocatedCells':     0,   # number of time rx-triggered 6top relocates a single cell
                'dropped6PFailedEnqueue':  0,   # dropped packets because 6P failed enqueue them
                'dropped6PFailedEnqueueLLSF':  0,   # dropped packets because 6P failed enqueue them
                'topGeneratedAdd':    0, # gdaneels
                'topGeneratedResponse':    0, # gdaneels
                'topGeneratedAddLLSF':    0, # gdaneels
                'topGeneratedResponseLLSF':    0, # gdaneels
                'topGeneratedDelete':    0, # gdaneels
                'topGeneratedDeleteResponse':    0, # gdaneels
                'topSuccessGeneratedAdd':    0, # gdaneels
                'topSuccessGeneratedResponse':    0, # gdaneels
                'topSuccessGeneratedAddLLSF':    0, # gdaneels
                'topSuccessGeneratedResponseLLSF':    0, # gdaneels
                'topSuccessGeneratedDelete':    0, # gdaneels
                'topSuccessGeneratedDeleteResponse':    0, # gdaneels
                'successful6PTransactionNormal': 0,
                'successful6PTransactionLLSF': 0,
                'successful6PTransactionLLSFRoot': 0,
                'successful6PTransactionLLSFIntermediate': 0,
                'sharedSlotsTransmits': 0,
                # tsch
                'droppedMacRetries':       0,   # packets dropped because more than TSCH_MAXTXRETRIES MAC retries
                'droppedMacRetriesTop':       0,   # packets dropped because more than TSCH_MAXTXRETRIES MAC retries

                'zSent6pData':  0,
                'zSent6pDataReSF':   0,
            }

    def _stats_incrementMoteStats(self,name):
        with self.dataLock:
            self.motestats[name] += 1

    # cell stats

    def getCellStats(self,ts_p,ch_p):
        ''' retrieves cell stats '''

        returnVal = None
        with self.dataLock:
            for (ts,cell) in self.schedule.items():
                if ts==ts_p and cell['ch']==ch_p:
                    returnVal = {
                        'dir':            cell['dir'],
                        'neighbor':       cell['neighbor'].id,
                        'numTx':          cell['numTx'],
                        'numTxAck':       cell['numTxAck'],
                        'numRx':          cell['numRx'],
                    }
                    break
        return returnVal

    # queue stats

    def _stats_logQueueDelay(self,delay):
        with self.dataLock:
            self.queuestats['delay'] += [delay]

    def _stats_getAveQueueDelay(self):
        d = self.queuestats['delay']
        return float(sum(d))/len(d) if len(d)>0 else 0

    def _stats_getAveQueueDelayStd(self):
        d = self.queuestats['delay']
        return float(np.std(np.array(d), axis = 0)) if len(d)>0 else 0

    def _stats_resetQueueStats(self):
        with self.dataLock:
            self.queuestats = {
                'delay':               [],
            }

    # latency stats

    def _stats_logLatencyStat(self,latency):
        with self.dataLock:
            self.packetLatencies += [latency]

    def _stats_getAveLatency(self):
        with self.dataLock:
            d = self.packetLatencies
            return float(sum(d))/float(len(d)) if len(d)>0 else 0

    def _stats_getAveLatencyStd(self):
        # if len(self.packetLatencies) > 0:
        #     print self.packetLatencies
        with self.dataLock:
            d = self.packetLatencies
            stdAvg = float(np.std(np.array(d), axis = 0)) if len(d)>0 else 0
            return stdAvg

    def _stats_resetLatencyStats(self):
        with self.dataLock:
            self.packetLatencies = []

    # etx

    def _stats_getAveETX(self):
        with self.dataLock:
            d = self.engine.etx
            avg = float(sum(d))/float(len(d)) if len(d)>0 else 0
            # print 'ETX values: %s' % self.engine.etx
            # print 'Avg ETX: %s' % avg
            return avg

    def _stats_getAveETXStd(self):
        with self.dataLock:
            d = self.engine.etx
            stdAvg = float(np.std(np.array(d), axis = 0)) if len(d)>0 else 0
            # print 'ETX values: %s' % self.engine.etx
            # print 'STD ETX: %s' % stdAvg
            return stdAvg

    def _stats_resetETXStats(self):
        with self.dataLock:
            self.engine.etx = []

    # hops stats

    def _stats_logHopsStat(self,hops):
        with self.dataLock:
            self.packetHops += [hops]

    def _stats_getAveHops(self):
        with self.dataLock:
            d = self.packetHops
            return float(sum(d))/float(len(d)) if len(d)>0 else 0

    def _stats_getAveHopsStd(self):
        # print "Hops of this cycle: %s" % str(self.packetHops)
        with self.dataLock:
            d = self.packetHops
            return float(np.std(np.array(d))) if len(d)>0 else 0

    def _stats_resetHopsStats(self):
        with self.dataLock:
            self.packetHops = []

    # radio stats

    def stats_incrementRadioStats(self,name):
        with self.dataLock:
            self.radiostats[name] += 1

    def _stats_getRadioStats(self,name):
        return self.radiostats[name]

    def _stats_resetRadioStats(self):
        with self.dataLock:
            self.radiostats = {
                'probableCollisions':      0,   # number of packets that can collide with another packets
            }

    #===== log

    def _log(self,severity,template,params=()):

        if   severity==self.DEBUG:
            if not log.isEnabledFor(logging.DEBUG):
                return
            logfunc = log.debug
        elif severity==self.INFO:
            if not log.isEnabledFor(logging.INFO):
                return
            logfunc = log.info
        elif severity==self.WARNING:
            if not log.isEnabledFor(logging.WARNING):
                return
            logfunc = log.warning
        elif severity==self.ERROR:
            if not log.isEnabledFor(logging.ERROR):
                return
            logfunc = log.error
        else:
            raise NotImplementedError()

        output  = []
        output += ['[ASN={0:>6} id={1:>4}] '.format(self.engine.getAsn(),self.id)]
        output += [template.format(*params)]
        output  = ''.join(output)
        logfunc(output)
