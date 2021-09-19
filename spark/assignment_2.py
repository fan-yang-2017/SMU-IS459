import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id
from graphframes import *

spark = SparkSession.builder.appName('sg.edu.smu.is459.assignment2').getOrCreate()

# Load data
posts_df = spark.read.load('/user/yangfan/parquet-input/hardwarezone.parquet')

# Clean the dataframe by removing rows with any null value
posts_df = posts_df.na.drop()

posts_df.createOrReplaceTempView("posts")

# Find distinct users
distinct_author = spark.sql("SELECT DISTINCT author FROM posts")
author_df = posts_df.select('author').distinct()

print('Author number :' + str(author_df.count()))

# Assign ID to the users
author_id = author_df.withColumn('id', monotonically_increasing_id())
author_id.show()

# Construct connection between post and author
dta = spark.sql("select distinct topic,author from posts")
rdta = dta.withColumnRenamed("topic","rtopic").withColumnRenamed("author","rauthor")

#  Self join on topic to build connection between authors
author_to_author = dta.join(rdta, dta.topic == rdta.rtopic).select(dta.author, rdta.rauthor).distinct()
edge_num = author_to_author.count()
print('Number of edges with duplicate : ' + str(edge_num))

# Convert it into ids
id_to_author = author_to_author \
    .join(author_id, author_to_author.author == author_id.author) \
    .select(author_to_author.rauthor, author_id.id) \
    .withColumnRenamed('id','src')

id_to_id = id_to_author \
    .join(author_id, id_to_author.rauthor == author_id.author) \
    .select(id_to_author.src, author_id.id) \
    .withColumnRenamed('id', 'dst')

id_to_id = id_to_id.filter(id_to_id.src >= id_to_id.dst).distinct()

id_to_id.cache()

print("Number of edges without duplciate :" + str(id_to_id.count()))

# Build graph with RDDs
graph = GraphFrame(author_id, id_to_id)

# For complex graph queries, e.g., connected components, you need to set
# the checkopoint directory on HDFS, so Spark can handle failures.
# Remember to change to a valid directory in your HDFS
spark.sparkContext.setCheckpointDir('/user/yangfan/spark-checkpoint')

# The rest is your work, guys
# ......
# num_connected_components = graph.connectedComponents().count()
# print("Number of connected_components :" + str(num_connected_components))

triangle_counts = graph.triangleCount()
# print(triangle_counts)
total_triangle_counts= triangle_counts.select('count').groupBy().sum().show()
total_triangles = 263995143
average_triangle_counts = total_triangles / triangle_counts.count()
print("The cohensive level is :" + str(average_triangle_counts)) 
