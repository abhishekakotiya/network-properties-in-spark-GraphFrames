import sys
import time
import networkx as nx
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import functions
from graphframes import *
from copy import deepcopy
import pandas

sc=SparkContext("local", "degree.py")
sqlContext = SQLContext(sc)

def articulations(g, usegraphframe=False):
	# Get the starting count of connected components
	start_count = g.connectedComponents().select('component').distinct().count()
	
	# Default version sparkifies the connected components process 
	# and serializes node iteration.
	articulatn = []
	if usegraphframe:
		# Get vertex list for serial iteration
		vertices = g.vertices.map(lambda v: v.id).collect()
	
		# For each vertex, generate a new graphframe missing that vertex
		# and calculate connected component count. Then append count to
		# the output
		for v in vertices:
			gframe = GraphFrame(g.vertices.filter('id !="' + v + '"'), g.edges.filter('src !="' + v + '"' and 'dst != "' + v + '"'))
			connected_count = gframe.connectedComponents().select('component').distinct().count()
			articulatn.append((v, 1) if connected_count > start_count else (v, 0))
		articulatn_df = sqlContext.createDataFrame(sc.parallelize(articulatn), ['id', 'articulation'])
		return articulatn_df
		
	# Non-default version sparkifies node iteration and uses networkx 
	# for connected components count.
	else:
		vertices = g.vertices.map(lambda v: v.id).collect()		
        	nx_graph = nx.Graph()
	        nx_graph.add_nodes_from(vertices)
        	nx_graph.add_edges_from(g.edges.map(lambda e: (e.src, e.dst)).collect())

		for v in vertices:
			temp_graph = deepcopy(nx_graph)        
        		temp_graph.remove_node(v)
        		articulatn.append((v, 1) if nx.number_connected_components(temp_graph) > start_count else (v, 0))
	        return sqlContext.createDataFrame(sc.parallelize(articulatn), ['id', 'articulation'])
		

filename = sys.argv[1]
lines = sc.textFile(filename)

pairs = lines.map(lambda s: s.split(","))
e = sqlContext.createDataFrame(pairs,['src','dst'])
e = e.unionAll(e.selectExpr('src as dst','dst as src')).distinct() # Ensure undirectedness 	

# Extract all endpoints from input file and make a single column frame.
v = e.selectExpr('src as id').unionAll(e.selectExpr('dst as id')).distinct()	

# Create graphframe from the vertices and edges.
g = GraphFrame(v,e)

#Runtime approximately 5 minutes
print("---------------------------")
print("Processing graph using Spark iteration over nodes and serial (networkx) connectedness calculations")
init = time.time()
df = articulations(g, False)
print("Execution time: %s seconds" % (time.time() - init))
print("Articulation points:")
df.filter('articulation = 1').show(truncate=False)
print("---------------------------")
df.toPandas().to_csv('articulations_out.csv')

#Runtime for below is more than 2 hours
#print("Processing graph using serial iteration over nodes and GraphFrame connectedness calculations")
#init = time.time()
#df = articulations(g, True)
#print("Execution time: %s seconds" % (time.time() - init))
#print("Articulation points:")
#df.filter('articulation = 1').show(truncate=False)
