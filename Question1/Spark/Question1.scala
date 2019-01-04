 import org.apache.spark.SparkContext
 import org.apache.spark.SparkConf
 import org.apache.spark.sql
 object Question1 {
 def store(){
 val df = spark.read.json("business.json");
 val df1 = df.withColumn("categories",explode(col("categories")));
 val df2 = df1.withColumn("city",lower(col("city")));
 val df3 = spark.read.csv("uscitiesv1.3.csv");
 val df4 = df3.withColumn("_c0",lower(col("_c0")));
 val df5 = df4.join(df2,df4("_c0")===df2("city"));
 val df6 = spark.read.json("review.json");
 val df7 = df6.join(df5,"business_id");
 val df8 = df7.select(col("user_id"),col("city"),col("business_id"),col("categories"));
 val df9 = df8.distinct;
 val df10 = df9.groupBy(col("city"),col("categories"));
 val df11 = df10.count;
 df11.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("spark_output_1");  
 }
 }

