# Yelp-Data-Analysis
Analyzed the Yelp dataset using Pig and Scala to get useful insights about the business and reviewers 

List of Yelp Data Analysis Questions are as follows:

[Yelp_Data_Analysis_Questions](Yelp_Data_Analysis_Questions.pdf)

## How to setup
### Executing Pig Script:
1. Move the Pig_Jar folder to HDFS
2. Move the pig script (filename.pig) to HDFS
3. Execute the pig script using the command pig filename.pig
4. The output path in the pig script should be a unique path, or it will throw an exception

### Executing Scala Script:
1. Move the Scala script (filename.scala) to HDFS
2. Load the scala script using the command :load filename.scala in the Spark shell
3. Call the method inside the scala script to be executed using the command filename.methodname;
4. The output path in the scala script should be a unique path, or it will throw an exception
