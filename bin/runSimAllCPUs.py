#!/usr/bin/python
'''
\brief Start batch of simulations concurrently.
Workload is distributed equally among CPU cores.
\author Thomas Watteyne <watteyne@eecs.berkeley.edu>
'''

import os
import time
import math
import multiprocessing
import sys

MIN_TOTAL_RUNRUNS = 1

def runOneSim(params):
    (cpuID,numRuns) = params
    command     = []
    command    += ['python runSimOneCPU.py']
    command    += ['--numRuns {0}'.format(numRuns)]
    command    += ['--cpuID {0}'.format(cpuID)]
    #command    += ['&']
    command     = ' '.join(command)
    os.system(command)

def runOneSimAutomated(params):
    (cpuID,numRuns,extraCommandParams) = params
    command     = []
    command    += ['python runSimOneCPU.py']
    command    += ['--numRuns={0}'.format(numRuns)]
    command    += ['--cpuID={0}'.format(cpuID)]
    #command    += ['&']
    command     = ' '.join(command)
    command    += ' '
    command    += extraCommandParams
    print command
    os.system(command)

def printProgress(num_cpus):
    while True:
        time.sleep(1)
        output     = []
        for cpu in range(num_cpus):
            if os.path.isfile('cpu{0}.templog'.format(cpu)):
                with open('cpu{0}.templog'.format(cpu),'r') as f:
                    output += ['[cpu {0}] {1}'.format(cpu,f.read())]
            else:
                output += ['[cpu {0}] no templog file yet'.format(cpu)]
        allDone = True
        for line in output:
            if line.count('ended')==0:
                allDone = False
        output = '\n'.join(output)
        #os.system('cls')
        # os.system('clear')
        print output
        if allDone:
            break
    for cpu in range(num_cpus):
        os.remove('cpu{0}.templog'.format(cpu))

def runSimAllCPUs(numRuns, extraCommandParams, numCPUs = -1):
    multiprocessing.freeze_support()
    num_cpus = numCPUs
    if numCPUs == -1:
        num_cpus = multiprocessing.cpu_count
    runsPerCpu = int(math.ceil(float(numRuns)/float(num_cpus)))
    assert (runsPerCpu * num_cpus == int(numRuns))
    pool = multiprocessing.Pool(num_cpus)
    pool.map_async(runOneSimAutomated,[(i,runsPerCpu, extraCommandParams) for i in range(num_cpus)])
    # time.sleep(5)
    printProgress(num_cpus)
    # raw_input("Done. Press Enter to close.")

if __name__ == '__main__':
    extraCommands = '%s %s %s %s %s %s %s %s %s %s %s %s %s %s %s %s %s %s' % (str(sys.argv[1]), str(sys.argv[2]), str(sys.argv[3]), str(sys.argv[4]), str(sys.argv[5]), str(sys.argv[6]), str(sys.argv[7]), str(sys.argv[8]), str(sys.argv[9]), str(sys.argv[10]), str(sys.argv[11]), str(sys.argv[12]), str(sys.argv[13]), str(sys.argv[14]), str(sys.argv[15]), str(sys.argv[16]), str(sys.argv[17]), str(sys.argv[18]))
    # print 'true or  false: %s' % sys.argv[11]
    numRuns = int(sys.argv[19])
    multiprocessing.freeze_support()
    num_cpus = multiprocessing.cpu_count()
    runsPerCpu = int(math.ceil(float(numRuns)/float(num_cpus)))
    assert (runsPerCpu * num_cpus == int(numRuns))
    pool = multiprocessing.Pool(num_cpus)
    pool.map_async(runOneSimAutomated,[(i,runsPerCpu, extraCommands) for i in range(num_cpus)])
    printProgress(num_cpus)
