from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import functions
from graphframes import *
from pyspark.sql.functions import explode
import pandas

sc=SparkContext("local", "degree.py")
sqlContext = SQLContext(sc)

def closeness(g):
	
	# Get list of vertices. We'll generate all the shortest paths at
	# once using this list.
	vertices = g.vertices.map(lambda v: v.id).collect()

	# first get all the path lengths.
	path_len = g.shortestPaths(landmarks=vertices)
	
	# Break up the map and group by ID for summing
	closen_df = path_len.select(explode('distances').alias('v', 'dist')).groupBy('v')
	
	# Sum by ID
	closen_df = closen_df.agg(functions.sum('dist').alias('dist_sum'))

	# Get the inverses and generate desired dataframe.
	closen_df = closen_df.selectExpr('v as id', '1/dist_sum as closeness')
	
	return closen_df


print("Reading in graph for problem 2.")
graph = sc.parallelize([('A','B'),('A','C'),('A','D'),
	('B','A'),('B','C'),('B','D'),('B','E'),
	('C','A'),('C','B'),('C','D'),('C','F'),('C','H'),
	('D','A'),('D','B'),('D','C'),('D','E'),('D','F'),('D','G'),
	('E','B'),('E','D'),('E','F'),('E','G'),
	('F','C'),('F','D'),('F','E'),('F','G'),('F','H'),
	('G','D'),('G','E'),('G','F'),
	('H','C'),('H','F'),('H','I'),
	('I','H'),('I','J'),
	('J','I')])
	
e = sqlContext.createDataFrame(graph,['src','dst'])
v = e.selectExpr('src as id').unionAll(e.selectExpr('dst as id')).distinct()
print("Generating GraphFrame.")
g = GraphFrame(v,e)

print("Calculating closeness.")
closeness_out = closeness(g).sort('closeness',ascending=False)
closeness_out.toPandas().to_csv('centrality_out.csv')
closeness_out.show()
