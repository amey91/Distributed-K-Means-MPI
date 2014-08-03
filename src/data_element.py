import math

class DataIO(object):

    def read_file(self, input_file):
        with open(input_file, "r") as f:
            content = f.read().splitlines()
        f.close()
        return content

    def write_file(self, output_file, centroids, membership):
        with open(output_file, "w") as f:
            for index, centroid in enumerate(centroids):
                f.write(repr(centroid))
                f.write('\n')
                cluster = membership[index]
                for element in cluster:
                    if (element != centroid):
                        f.write(repr(element))
                        f.write('\n')
                f.write('\n')
        f.close()

    def append_file(self, output_file, cluster):
        with open(output_file, "a") as f:
            for element in cluster:
                    f.write(repr(element))
                    f.write('\n')
            f.write('\n')
        f.close()

    def merge_files(self, inputfile1, inputfile2, outputfile):
        file_names = []
        file_names.append(inputfile1)
        file_names.append(inputfile2)
        # referred to http://stackoverflow.com/questions/13613336/python-concatenate-text-files
        with open(outputfile, 'w') as outfile:
            for fname in file_names:
                with open(fname) as infile:
                    for line in infile:
                        outfile.write(line)
        outfile.close()

    def break_file(self, numOfParts, inputFile):
        result = []
        linesRead = 0
        blocksRead = 0 
        content = self.read_file(inputFile)
        fileSize = len(content)
        blockSize = int(fileSize / numOfParts)
        while blocksRead < numOfParts and linesRead < fileSize:
            #append block to result
            res = content[linesRead:linesRead + blockSize]
            if len(res) > 0:
                result.append(res)
            #update current pointer
            blocksRead += 1
            linesRead += blockSize
        #write the last few lines into last block
        if linesRead < fileSize:
            remainingcontent = content[linesRead:]
            iiter = 0
            for item in remainingcontent:
                if len(item) > 0:
                    result[iiter].append(item)
                iiter = (iiter + 1) % (len(result) - 1)
        return result


class DataElement(object):

    def __init__(self):
        self.cluster = -1

    def distance(self, other):
        raise NotImplementedError()

    def set_cluster(self, cluster):
        self.cluster = cluster


class Point2D(DataElement):

    def __init__(self, x, y):
        super(Point2D, self).__init__()
        self.x = x
        self.y = y

    def __repr__(self):
        return "x: %s, y: %s" % (self.x, self.y)

    def _euclidian_distance(self, x1, y1, x2, y2):
        '''Computes the distance between two 2D points'''
        return math.sqrt(math.pow((x1 - x2), 2) + \
                    math.pow((y1 - y2), 2))

    def distance(self, other):
        return self._euclidian_distance(self.x, self.y, other.x, other.y)


class DNA(DataElement):

    def __init__(self, strand):
        super(DNA, self).__init__()
        self.strand = strand

    def __repr__(self):
        return self.strand

    def distance(self, other):
        return self._hamming_distance(self.strand, other.strand)

    def _hamming_distance(self, strand1, strand2):
        '''
        Computers the distance between 2 DNA strands.
        Hamming distance = number of non-matching characters
        '''
        diff = len(strand1)
        for i in range(0, len(strand1)):
            if strand1[i] == strand2[i]:
                diff -= 1
        # return number of positions that do not overlap
        return diff
