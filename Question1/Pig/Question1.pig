REGISTER Pig_Jar/json-simple-1.1.1.jar;
REGISTER Pig_Jar/elephant-bird-pig-4.1.jar;
REGISTER Pig_Jar/elephant-bird-core-4.1.jar;
REGISTER Pig_Jar/elephant-bird-hadoop-compat-4.10.jar;
REGISTER Pig_Jar/piggybank.jar;

BUSINESS = LOAD 'business.json' USING com.twitter.elephantbird.pig.load.JsonLoader('-nestedLoad') AS jsonmap_2;
a = FOREACH BUSINESS GENERATE jsonmap_2#'business_id' AS Business_ID,
    LOWER(jsonmap_2#'city') AS City,
    FLATTEN(jsonmap_2#'categories') AS Categories;

USCITY = LOAD 'uscitiesv1.3.csv' using org.apache.pig.piggybank.storage.CSVLoader() as (city);
b = FOREACH USCITY GENERATE LOWER(city) AS City;

c = JOIN a BY City,b BY City;
      
REVIEW = LOAD 'review.json' USING com.twitter.elephantbird.pig.load.JsonLoader('-nestedLoad') AS jsonmap;
d = FOREACH REVIEW GENERATE jsonmap#'user_id' AS User_ID,
    jsonmap#'business_id' AS Business_ID;

e = JOIN c BY a::Business_ID,d BY Business_ID;

f = FOREACH e GENERATE d::User_ID AS User_ID,
    c::a::City AS City,
    c::a::Business_ID AS Business_ID, 
    c::a::Categories AS Categories;

g = DISTINCT f;

h = GROUP g by (City,Categories);

i = FOREACH h GENERATE FLATTEN(group) AS (City,Categories),
    COUNT(g.User_ID) AS Number_Of_Unique_Reviewers;

STORE i INTO 'output1' USING PigStorage('\t');
   	