import sys
import numpy
import getopt
import random
from random import sample

possible_bases = ['A', 'C', 'G', 'T']

def usage():
    print '$> python generaterawdata_dna.py <required args> \n' + \
        '\t-c <#>\t\tNumber of clusters to generate\n' + \
        '\t-d <#>\t\tNumber of DNA strands per cluster\n' + \
        '\t-b <#>\t\tNumber of bases in each DNA strand\n' + \
        '\t-o <file>\tFilename for the output of the raw data\n'   

# python generaterawdata.py -c 5 -d 100 -b 15 -o  out.txt

def strandDistance(strand1, strand2):
    '''
    Computers the distance between 2 DNA strands.
    distance = number of non-matching characters
    '''
    diff = len(strand1)
    for i in range(0, len(strand1)):
        if strand1[i] == strand2[i]:
            diff -= 1
    # return number of positions that do not overlap
    return diff


def tooClose(point, points, minDist):
    '''
    Determine if strands are sufficiently different
    '''
    if len(point) < minDist:
        minDist = len(point)
    for pair in points:
        if strandDistance(point, pair) < minDist:
            return True
    return False

def handleArgs(args):
    # set up return values
    numClusters = -1
    numStrands = -1
    numBasesPerStrand = -1
    output = None

    try:
        optlist, args = getopt.getopt(args[1:], 'c:d:b:o:')
    except getopt.GetoptError, err:
        print str(err)
        usage()
        sys.exit(2)

    for key, val in optlist:
        # first, the required arguments
        if   key == '-c':
            numClusters = int(val)
        elif key == '-d':
            numStrands = int(val)
        elif key == '-b':
            numBasesPerStrand = int(val)
        elif key == '-o':
            output = val

    # check required arguments were inputted  
    if numClusters < 0 or numStrands < 0 or \
            numBasesPerStrand < 0 or \
            output is None:
        usage()
        sys.exit()
    return (numClusters, numStrands, numBasesPerStrand, output)

def drawOrigin(maxValue):
    return numpy.random.uniform(0, maxValue, 2)

def getStrand(bases):
    strand = ""
    for j in range(0, bases):
        strand += str(random.choice(possible_bases))
    return strand

def vary_strand(centroid, variance):
    # if strand is too small ( <5) 
    if variance > len(centroid):
        variance = len(centroid)

    for _ in range(0, variance):
        pos = random.randrange(0, len(centroid))
        new_char = random.choice(possible_bases)
        #till we get a different character than already present
        while(new_char == centroid[pos]):
            new_char = random.choice(possible_bases)
        temp = list(centroid)
        temp[pos] = new_char
        centroid = "".join(temp)
    return centroid


# start by reading the command line
numClusters, \
numStrands, \
numBasesPerStrand, \
output = handleArgs(sys.argv)


writer = open(output, "w")

# step 1: generate each dna centroid
centroid_strands = []
minDistance = int(numBasesPerStrand/2)

for i in range(0, numClusters):
    strand = getStrand(numBasesPerStrand)
    # is it far enough from the others?
    while (tooClose(strand, centroid_strands, minDistance)):
        strand = getStrand(numBasesPerStrand)
    centroid_strands.append(strand)


# step 2: generate the points for each centroid
points = []
minClusterVar = 1 # min characters that diverge from centroid
maxClusterVar = 4 # max characters that diverge from centroid
#maxClusterVar = numBasesPerStrand/2
rowsWritten = 0
for i in range(0, numClusters):
    # compute the variance for this cluster
    variance = numpy.random.uniform(minClusterVar, maxClusterVar)
    centroid = centroid_strands[i]
    # write centroid to file
    writer.write(centroid)
    writer.write('\n')
    rowsWritten += 1
    print "centroid = " , centroid, " variance = " , str(int(variance))

    # for each centroid, create mutations
    for j in range(0, numStrands - 1):
        # generate a DNA strand with specified variance
        # should be normally-distributed around centroids[i]
        new_strand = vary_strand(centroid, int(variance))
        # write the results out
        writer.write(new_strand)
        writer.write('\n')
        rowsWritten += 1

writer.close()