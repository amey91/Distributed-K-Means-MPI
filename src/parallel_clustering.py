"""referred to: https://github.com/jbornschein/mpi4py-examples/blob/master/09-task-pull.py
"""

from mpi4py import MPI
import sys
import data_element as de
import getopt
import random

def enum(*sequential, **named):
    '''referred to http://stackoverflow.com/questions/36932/how-can-i-represent-an-enum-in-python 
    '''
    enums = dict(zip(sequential, range(len(sequential))), **named)
    return type('Enum', (), enums)

# Define MPI message tags
tags = enum('INIT', 'SEND_INIT_CENTROID', 'SEND_CENTROID', 'CENTROID_LIST', 'SEND_DATA', 'SEND_RESULT', 'EXIT')


comm = MPI.COMM_WORLD
rank = comm.Get_rank()
name = MPI.Get_processor_name()
num_workers = comm.size
status = MPI.Status()
num_clusters = num_workers - 1

if rank > 0:
    # if a worker, then create a local cluster
    membership = [[]]
    local_cluster = []
    objects = []

centroids = []
iteration = 0

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

def assign_cluster(objects, centroids):
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

def get_objects(data_type, data):
    objects = []
    if data_type == 'point':
        '''Point2D'''
        for elem in data:
            coords = elem.split(',')
            x = float(coords[0])
            y = float(coords[1])
            objects.append(de.Point2D(x, y))
    if data_type == 'dna':
        '''DNA'''
        for elem in data:
            objects.append(de.DNA(elem))
    return objects

def append_to_result(source, data):
    de.DataIO().append_file(output_file, data)
    #print data

#####

def usage():
    print '$> python parallel_clustering.py <required args>\n' + \
        '\t-d <point|dna>\tData type\n' + \
        '\t-t <#>\t\tThreshold\n' + \
        '\t-i <file>\tFilename for the data input\n' + \
        '\t-o <file>\tFilename for the results output\n'

def handle_args(args):
    # set up return values
    data_type = None
    threshold = -1
    input_file = None
    output_file = None

    try:
        optlist, args = getopt.getopt(args[1:], 'd:t:i:o:')
    except getopt.GetoptError, err:
        print str(err)
        usage()
        sys.exit(2)

    for key, val in optlist:
        # required arguments
        if key == '-d':
            data_type = val
        elif key == '-t':
            threshold = int(val)
        elif key == '-i':
            input_file = val
        elif key == '-o':
            output_file = val

    # check required arguments were inputted  
    if data_type is None or threshold < 0 or \
            input_file is None or output_file is None:
        usage()
        sys.exit()
    return (data_type, threshold, input_file, output_file)


######
# start by reading the command line
data_type, \
threshold, \
input_file, \
output_file = handle_args(sys.argv)
content = de.DataIO().break_file(num_clusters, input_file)

if rank == 0:
    # Master process executes code below
    converged_workers = 0
    centroids = [-1] * num_clusters
    print("Master starting with %d workers" % num_clusters)

    # respond to init request by sending initial data
    for i in range(1, num_workers):
        data = comm.recv(source=MPI.ANY_SOURCE, tag=tags.INIT, status=status)
        source = status.Get_source()
        # print "len content =", len(content), 'being referenced = ', source-1
        ''' worker ready, send block of file to worker '''
        comm.send(content[source - 1], dest=source, tag=tags.SEND_DATA)
        print "Sending init data %s to worker %d" % (content[source - 1], source)

    # receive initial centroids
    for i in range(1, num_workers):
        data = comm.recv(source=MPI.ANY_SOURCE, tag=tags.SEND_INIT_CENTROID, status=status)
        source = status.Get_source()
        # worker ready, send block of file to worker
        centroids[source - 1] = data
        print "Received initial centroid %s from %d worker" % (data, source)

    while True:
        #broadcast centroid list
        for i in range(1, num_workers):
            comm.send(centroids, dest=i, tag=tags.CENTROID_LIST)
        #clear centroid list
        centroids = [-1] * num_clusters
        comm.Barrier()
        # receive new centroids
        for i in range (1, num_workers):
            data = comm.recv(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG, status=status)
            source = status.Get_source()
            tag = status.Get_tag()
            if tag != tags.SEND_CENTROID:
                raise Exception("UNEXPECTED TAG AT LOCATION 129")
            else:
                centroids[source - 1] = data
                print "Received recalculated centroid %s from %d worker" % (data, source)

        iteration += 1
        if iteration >= threshold:
            # broadcast end message
            for i in range(1, num_workers):
                comm.send(centroids, dest=i,tag=tags.EXIT)

            #take final result from workers
            for i in range(1, num_workers):
                data = comm.recv(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG, status=status)
                source = status.Get_source()
                tag = status.Get_tag()
                if tag != tags.SEND_RESULT:
                    raise Exception("UNEXPECTED TAG AT LOCATION 129")
                else:
                    append_to_result(source, data)
            print centroids
            print "Done\n"
            break
else:
    # worker processes execute code below
    name = MPI.Get_processor_name()
    print "Worker starting with rank %d on %s, from total of %d children..." % (rank, name, num_workers)
    comm.send(None, dest=0,tag=tags.INIT)
    # receive the initial cluster
    data = comm.recv(source=0, tag=tags.SEND_DATA, status=status)
    objects = get_objects(data_type, data)

    local_centroid = get_cluster_centroid(objects)
    # TODO find centroid of that cluster 

    comm.send(local_centroid, dest=0, tag=tags.SEND_INIT_CENTROID)

    # do till you receive EXIT status
    while True:
        data = comm.recv(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG, status=status)
        source = status.Get_source()
        tag = status.Get_tag()
        print "worker %d got main message from %d withtag %s" %(rank, source, tag)
        # message is from master
        if tag == tags.EXIT:
            comm.send(local_cluster, dest=0, tag=tags.SEND_RESULT)
            print "Worker %d exiting" % rank
            break
        elif tag == tags.CENTROID_LIST:
            comm.Barrier()
            ''' start centroid update routine '''
            centroids = data
            membership = assign_cluster(objects, centroids)
            local_cluster = membership[rank - 1]
            # receive new items for local cluster
            for _ in range(1, rank):
                data = comm.recv(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG, status=status)
                source = status.Get_source()
                print "Worker %d got message from %d" %(rank, source)
                for item in data:
                    local_cluster.append(item)
            # send new clusters to other workers
            for i in range(1, num_workers):
                if i != rank:
                    print "Worker %d sends data to %d" % (rank, i)
                    comm.send(membership[i - 1], dest=i, tag=tags.SEND_DATA)
            # keep receiving new items for local cluster
            for i in range(rank + 1, num_workers):
                data = comm.recv(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG, status=status)
                source = status.Get_source()
                print "2. worker %d got message from %d" %(rank, source)
                for item in data:
                    local_cluster.append(item)

            objects = local_cluster
            local_centroid = get_cluster_centroid(local_cluster)
            #send new centroid to source
            comm.send(local_centroid, dest = 0, tag=tags.SEND_CENTROID)
            ''' end centroid update routine '''
