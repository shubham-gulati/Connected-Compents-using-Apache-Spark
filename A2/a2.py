"""
First Name : Shubham
Last Name : Gulati
UBIT Name : sgulati3
"""

from pyspark import SparkConf, SparkContext
import sys, time

def initialization(data) :
    split_data = data.split(" ")
    v = [ int(x) for x in split_data ]
    yield v[0], v[1]
    yield v[1], v[0]

def MapOperationlargeStar(data):
    u,v = data
    yield (u,v)
    yield (v,u)

def MapOperationsmallStar(data):
    u,v = data
    if v < u:
        yield u,v
    else:
        yield v,u

def ReducelargeStar(data):
    u, v = data
    neighbour_list = list(v)
    neighbour_list.append(u)
    min_elem = min(neighbour_list)
    for elem in v:
        if u < elem:
            yield elem, min_elem

def ReducesmallStar(data):
    u, v = data
    v = list(v)
    v.append(u)
    min_elem = min(v)
    for elem in v:
        if elem != min_elem:
            yield elem, min_elem


if __name__ == "__main__":

    conf = SparkConf().setAppName("RDDcreate")
    sc = SparkContext(conf = conf)
    lines = sc.textFile(sys.argv[1])
    prevRDD = lines.flatMap(initialization)

    # Two phase algorithm. Calculate Large star until converge. And then find small star until converge.
    while True:
        while True:
            newRDD = prevRDD.flatMap(MapOperationlargeStar).groupByKey().flatMap(ReducelargeStar)
            difference = (prevRDD.subtract(newRDD).union(newRDD.subtract(prevRDD))).count()
            prevRDD = newRDD
            if difference == 0:
                break;
            
        newRDD = prevRDD.flatMap(MapOperationsmallStar).groupByKey().flatMap(ReducesmallStar)
        changes = (prevRDD.subtract(newRDD).union(newRDD.subtract(prevRDD))).count()
        prevRDD = newRDD
        if changes == 0:
            break

    #Here we count the connected components and we add edges for those vertices in the final RDD
    comp_rdd = prevRDD.values().distinct()
    prevRDD = prevRDD.union(comp_rdd.map(lambda k: (k,k)))
    prevRDD.map(lambda args: "{0} {1}".format(args[0],args[1])).coalesce(1, shuffle = True).saveAsTextFile("OutputData")
    sc.stop()
