from data_element import DataIO
from data_element import DNA
from data_element import Point2D
import getopt
import random
import sys


data_type = None
objects = []
centroids = []
membership = [[]]


def usage():
    print '$> python sequencial_clustering.py <required args>\n' + \
        '\t-d <point|dna>\tData type\n' + \
        '\t-c <#>\t\tNumber of clusters\n' + \
        '\t-t <#>\t\tThreshold\n' + \
        '\t-i <file>\tFilename for the data input\n' + \
        '\t-o <file>\tFilename for the results output\n'

def handle_args(args):
    # set up return values
    data_type = None
    num_clusters = -1
    threshold = -1
    input_file = None
    output_file = None

    try:
        optlist, args = getopt.getopt(args[1:], 'd:c:t:i:o:')
    except getopt.GetoptError, err:
        print str(err)
        usage()
        sys.exit(2)

    for key, val in optlist:
        # required arguments
        if key == '-d':
            data_type = val
        elif key == '-c':
            num_clusters = int(val)
        elif key == '-t':
            threshold = int(val)
        elif key == '-i':
            input_file = val
        elif key == '-o':
            output_file = val

    # check required arguments were inputted  
    if data_type is None or num_clusters < 0 or threshold < 0 or \
            input_file is None or output_file is None:
        usage()
        sys.exit()
    return (data_type, num_clusters, threshold, input_file, output_file)

'''Returns new centroid for a cluster'''
def get_cluster_centroid(cluster):
    if not cluster:
        return objects[random.randint(0, len(objects) - 1)]
    min_sum = sys.maxint
    centroid = None
    for elem1 in cluster:
        sum_dist = 0
        for elem2 in cluster:
            sum_dist += elem1.distance(elem2)
        if sum_dist < min_sum:
            min_sum = sum_dist
            centroid = elem1
    return centroid

'''Returns list of centroids for each cluster'''
def update_centroids(num_clusters, membership):
    centroids = []
    for index in range(num_clusters):
        centroid = get_cluster_centroid(membership[index])
        centroids.append(centroid)
    return centroids

def get_initial_centroids(num_clusters, input_list):
    membership = [[] for _ in range(num_clusters)]
    '''Randomly assign membership and get centroids'''
    for index, elem in enumerate(input_list):
        cluster = random.randint(0, num_clusters - 1)
        membership[cluster].append(elem)
    return update_centroids(num_clusters, membership)

'''Assign each object to a cluster'''
def assign_clusters(num_clusters, centroids):
    membership = [[] for _ in range(num_clusters)]
    for elem in objects:
        min_index = -1
        min_distance = sys.maxint
        for index, centroid in enumerate(centroids):
            curr_distance = centroid.distance(elem)
            if curr_distance < min_distance:
                min_distance = curr_distance
                min_index = index
        membership[min_index].append(elem)
    return membership

'''Read objects from file'''
def get_objects(data_type, input_file):
    objects = []
    content = DataIO().read_file(input_file)
    if data_type == 'point':
        '''Point2D'''
        for elem in content:
            coords = elem.split(',')
            x = float(coords[0])
            y = float(coords[1])
            objects.append(Point2D(x, y))
    if data_type == 'dna':
        '''DNA'''
        for elem in content:
            objects.append(DNA(elem))
    return objects


# start by reading the command line
data_type, \
num_clusters, \
threshold, \
input_file, \
output_file = handle_args(sys.argv)

objects = get_objects(data_type, input_file)

'''Actual k-means algorithm'''
centroids = get_initial_centroids(num_clusters , objects)

i = 0
while True:
    i += 1
    membership = assign_clusters(num_clusters, centroids)
    centroids = update_centroids(num_clusters, membership)
    movement = 0
    for index, centroid in enumerate(centroids):
        if index != centroid.cluster:
            centroid.set_cluster(index)
            movement += 1
    for index, cluster in enumerate(membership):
        for elem in cluster:
            if index != elem.cluster:
                elem.set_cluster(index)
                movement += 1
    # movement /= len(objects)
    #if movement <= threshold:
    if i >= threshold:
        break

'''Save results to output file'''
print centroids
DataIO().write_file(output_file, centroids, membership)
print "Done\n"
