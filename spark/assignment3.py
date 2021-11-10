from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import explode
from pyspark.sql.functions import split, concat_ws, substring, from_json, col, lower, regexp_replace, window, current_timestamp, desc
from pyspark.ml.feature import StopWordsRemover, RegexTokenizer, Tokenizer


if __name__ == "__main__":

    spark = SparkSession.builder \
               .appName("KafkaWordCount") \
               .getOrCreate()

    #Read from Kafka's topic scrapy-output
    df = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("subscribe", "scrapy-output") \
            .option("startingOffsets", "earliest") \
            .load()

    #Parse the fields in the value column of the message
    lines = df.selectExpr("CAST(value AS STRING)", "timestamp")

    #Specify the schema of the fields
    hardwarezoneSchema = StructType([ \
        StructField("topic", StringType()), \
        StructField("author", StringType()), \
        StructField("content", StringType()) \
        ])

    #Use the function to parse the fields
    # lines = parse_data_from_kafka_message(lines, hardwarezoneSchema) \
    #     .select("topic","author","content","timestamp")
    lines = lines.withColumn('data', from_json(col("value"), schema=hardwarezoneSchema)).select('timestamp', 'data.*')


    # Top-10 users with most posts in 2 minutes

    users_df = lines.select("timestamp", "author") \
        .groupBy(window("timestamp", "2 minutes", "1 minute"), "author").count() \
        .withColumn("start", col("window")["start"]) \
        .withColumn("end", col("window")["end"]) \
        .withColumn("current_timestamp", current_timestamp()) \
        .filter(users_df.end < users_df.current_timestamp) \
        .orderBy(desc('window'), desc("count")).limit(10)

    # remove stop words and punctuations

    punct = lines.select('timestamp', split(lower(regexp_replace('content', r'[^\w\s]',''))," ").alias('content'))
    stopwordList = (["\n","\t", "content", "click", "expand", "", " ", "..."])
    stopwordList.extend(StopWordsRemover().getStopWords())
    stopwordList = list(set(stopwordList))
    remover = StopWordsRemover(inputCol='content', outputCol="cleaned_content", stopWords=stopwordList)
    cleaned_df = remover.transform(punct)


    # Splitting into individual words
    cleaned_df = cleaned_df.select("timestamp", concat_ws(" ", "cleaned_content").alias("individual_word")) \
        .select("timestamp", explode(split("individual_word", " ")).alias("content")) \
        .groupBy(window("timestamp", "2 minutes", "1 minute"), "content").count() \
        .withColumn("start", col('window')['start']) \
        .withColumn("end", col('window')['end']) \
        .withColumn("current_timestamp", current_timestamp()) \
        .filter(cleaned_df.end < cleaned_df.current_timestamp)
        .orderBy('window', 'count', ascending=False).limit(10)

    # #Select the content field and output
    contents1 = users_df \
        .writeStream \
        .queryName("WriteContent1") \
        .trigger(processingTime="1 minute") \
        .outputMode("complete") \
        .format("console") \
        .option("checkpointLocation", "/user/chesteroby/spark-checkpoint1") \
        .start()

    #Select the content field and output
    contents2 = cleaned_df \
        .writeStream \
        .queryName("WriteContent2") \
        .trigger(processingTime="1 minute") \
        .outputMode("complete") \
        .format("console") \
        .option("checkpointLocation", "/user/chesteroby/spark-checkpoint2") \
        .start()

    #Start the job and wait for the incoming messages
    contents2.awaitTermination()
