from pyspark import SparkConf, SparkContext
import sys, time

def initialization(data) :
    split_data = data.split(" ")
    v = [ int(x) for x in split_data ]
    yield v[0], v[1]
    yield v[1], v[0]

def MapOperationlargeStar(data) :
    u,v = data
    yield (u,v)
    yield (v,u)

def MapOperationsmallStar(data) :
    u,v = data
    if v <= u:
        yield u,v
    else:
        yield v,u

def ReducelargeStar(data):
    u, v = data
    neighbour_list = list(v)
    neighbour_list.append(u)
    min_elem = min(neighbour_list)
    for elem in v:
        if u <= elem:
            yield elem, min_elem

def ReducesmallStar(data):
    u, v = data
    v = list(v)
    v.append(u)
    min_elem = min(v)
    for elem in v:
        yield elem, min_elem

if __name__ == "__main__":

    start = time.time()
    conf = SparkConf().setAppName("RDDcreate")
    sc = SparkContext(conf = conf)
    #sc.setLogLevel("ERROR")
    lines = sc.textFile(sys.argv[1])
    flag = True;
    
    #Alternating algorithm. Calculate Large star and small star until converge
    while True:
        if flag:
            LRDD = sc.textFile(sys.argv[1]).flatMap(initialization).groupByKey().flatMap(ReducelargeStar).distinct()
        else:
            LRDD = SRDD.flatMap(MapOperationlargeStar).groupByKey().flatMap(ReducelargeStar).distinct()
        flag = False;
            
        SRDD = LRDD.flatMap(MapOperationsmallStar).groupByKey().flatMap(ReducesmallStar).distinct()
        changes = (LRDD.subtract(SRDD).union(SRDD.subtract(LRDD))).count()
        if changes == 0:
            break

    SRDD.map(lambda args: "{0} {1}".format(args[0],args[1])).coalesce(1).saveAsTextFile("Output")
    done = time.time()
    elapsed_time = done - start
    print(elapsed_time)
    sc.stop()
