 import org.apache.spark.SparkContext
 import org.apache.spark.SparkConf
 import org.apache.spark.sql
 object Question2 {
 def store(){
 val df1 = spark.read.json("business.json");
 val df2 = df1.withColumn("categories",explode(col("categories")));
 val df3 = df2.groupBy("categories","city");
 val df4 = df3.avg("stars");
 df4.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("spark_output_2");
 }
 }