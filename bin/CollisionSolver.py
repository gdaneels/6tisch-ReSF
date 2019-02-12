from math import ceil, floor
import random
from fractions import gcd
import sys

class CollisionSolver:
    def  __init__(self):
        pass

    def findGCD(self,a,b):
    # greatest common dividor
        while b != 0:
            temp = b
            b = a % b
            a = temp
        return a

    def findDiophantineCoefficients(self, start_one, interval_one, start_two, interval_two):
    # starting from: start_one + interval_one*m = start_two + interval_two*n
    # diophantine equation: interval_one*m - interval_two*n = start_two - start_one
    # thus a = interval_one
    # b = - interval_two
    # c = start_two - start_one
        return interval_one, -interval_two, start_two-start_one

    # stolen from https://www.math.utah.edu/~carlson/hsp2004/PythonShortCourse.pdf
    # recursive linear diophantine equation solver
    def solveDiophantineEquation(self, a,b,c):
        q, r = divmod(a,b)
        if r == 0:
            return( [0,c/b] )
        else:
            sol = self.solveDiophantineEquation( b, r, c )
            u = sol[0]
            v = sol[1]
            return( [ v, u - q*v ] )

    def hasSolution(self, a, b, c):
    # is the diophantine equation solvable? are there solutions?
        return (c % self.findGCD(a,b) == 0)

    def getCollisions(self, start_one, end_one, interval_one, start_two, end_two, interval_two):

        collisions = []
        sequence_one = lambda x: start_one + interval_one * x # sequence one formula
        sequence_two = lambda y: start_two + interval_two * y # sequence two formula

        # find the linear Diophantine equation coefficients
        a, b, c = self.findDiophantineCoefficients(start_one, interval_one, start_two, interval_two)

        # 1) check if there are solutions
        # (c % self.findGCD(a,b) == 0)
        # if not self.hasSolution(a, b, c):
        # t0 = time.time()
        gcdValue = gcd(a,b)
        # t1 = time.time()
        if not (c % gcdValue == 0):
            return []

        # 2) if there are solutions, there are infinitly many
        # find the specific (x_0, y_0)
        x_zero, y_zero = self.solveDiophantineEquation(a, b, c)

        # 3) determine the general solution format

        x = lambda n: x_zero + n * (b / float(gcdValue))
        y = lambda n: y_zero - n * (a / float(gcdValue))

        start_max = max(start_one, start_two)
        end_min = min(end_one, end_two)

        # 4) find intersection of start_two with sequence_one, and search for n
        # n_start = (((start_two - start_one) / float(interval_one)) - x_zero) / float((float(b) / float(gcdValue)))
        n_start = (((start_max - start_one) / float(interval_one)) - x_zero) / float((float(b) / float(gcdValue)))

        # 5) find intersection of end_two with sequence_two, and search for n
        # n_end = (((end_two - start_one) / float(interval_one)) - x_zero) / float((float(b) / float(gcdValue)))
        n_end = (((end_min - start_one) / float(interval_one)) - x_zero) / float((float(b) / float(gcdValue)))

        if n_start > n_end:
            raise -1

        # 6) find the n values that result in collisions when filled in x and y and are between start_two and end_two
        # n_list = []
        n_start_int = int(ceil(n_start))
        n_end_int = int(floor(n_end))
        # while n_start_int <= n_end_int:
        #     n_list.append(n_start_int)
        #     n_start_int += 1

        # nrCollisions = n_end_int - n_start_int + 1

        # print n_start_int
        # print n_end_int

        collisions = []
        # 7) calculate the actual collision ASNs

        quotientB = (b / float(gcdValue))
        collisions = [start_one + interval_one * (x_zero + n * quotientB) for n in range(n_start_int, n_end_int+1)]

        # for n in range(n_start_int, n_end_int+1):
        #     nx_seq = sequence_one(x(n))
        #     ny_seq = sequence_two(y(n))
        #     if nx_seq == ny_seq:
        #         # if nx_seq >= start_one and nx_seq >= start_two and nx_seq <= end_one and nx_seq <= end_two:
        #         collisions.append(nx_seq)
        #     else:
        #         print nx_seq
        #         print ny_seq
        #         assert False

        return collisions
#
# solver = CollisionSolver()
# print solver.getCollisions(1, 25, 2, 5, 25, 5)
# # print solver.getCollisions(6, 60, 5, 24, 72, -22)
# print solver.getCollisions(1,389,4,1,231,3)
# # print solver.solveDiophantineEquation(5, 22, 18)
# print len(solver.getCollisions(5334, 15332, 300, 5334, 15332, 300))

# listX = []
# x = 5334
# y = 15332
# while x < y:
#     listX.append(x)
#     x += 300
# print listX
# print len(listX)

def main():
    pass
    solver = CollisionSolver()
    # random.seed(1)
    # count = 0
    # while count < 100000:
    #     print 'TO GO: %d' % (100000 - count)
    #     endOne = int(sys.argv[1])
    #     # startOne = 95333
    #     # intervalOne = 1331
    #     # startTwo = 46942
    #     # intervalTwo = 5936
    #     endTwo = int(sys.argv[2])
    #     startOne = random.randint(1, 100000)
    #     intervalOne = random.randint(1, 10000)
    #     startTwo = random.randint(1, 100000)
    #     intervalTwo = random.randint(1, 10000)
    #     print '(%d, %d, %d, %d, %d, %d)' % (startOne, endOne, intervalOne, startTwo, endTwo, intervalTwo)
    #     # endTwo = 10000000
    #     # 95333, 2000000, 1331, 46942, 10000000, 5936
    #     # 95333, 2000000, 1331, 46942, 10000000, 5936
    #     # print 'FIRST LIST:'
    #     firstList = []
    #     startCount = startOne
    #     while startCount <= endOne:
    #         firstList.append(startCount)
    #         startCount += intervalOne
    #     # print sorted(firstList)
    #
    #     # print 'SECOND LIST:'
    #     secondList = []
    #     startCountTwo = startTwo
    #     while startCountTwo <= endTwo:
    #         secondList.append(startCountTwo)
    #         startCountTwo += intervalTwo
    #
    #     bruteForce = sorted(list(set(firstList).intersection(secondList)))
    #     solver = solverCollisions.getCollisions(startOne, endOne, intervalOne, startTwo, endTwo, intervalTwo)
    #     # print solver
    #     print '(%d, %d, %d, %d, %d, %d)' % (startOne, endOne, intervalOne, startTwo, endTwo, intervalTwo)
    #
    #     if sorted(solver) != sorted(bruteForce):
    #         print 'Brute force collision list: %s' % sorted(bruteForce)
    #         print 'Solver collision list: %s' % sorted(solver)
    #         print '(%d, %d, %d, %d, %d, %d)' % (startOne, endOne, intervalOne, startTwo, endTwo, intervalTwo)
    #         raise -1
    #     count += 1

    # print solver.getCollisions(6, 60, 5, 24, 72, -22)
    # print solver.getCollisions(1, 25, 2, 5, 25, 5)
    print solver.getCollisions(0, 10000, 100, 0, 10000, 1000)
    # print solver.getCollisions(113635, 151500, 5900, 114235, 119535, 5300)
    # print solver.getCollisions(5334, 15332, 300, 5334, 15332, 300)
    # print solver.getCollisions(122121,151500,7100, 105820,151500,7200)
if __name__=="__main__":
    main()
