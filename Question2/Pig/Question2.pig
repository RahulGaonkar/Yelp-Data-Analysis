REGISTER Pig_Jar/json-simple-1.1.1.jar;
REGISTER Pig_Jar/elephant-bird-pig-4.1.jar;
REGISTER Pig_Jar/elephant-bird-core-4.1.jar;
REGISTER Pig_Jar/elephant-bird-hadoop-compat-4.10.jar;

BUSINESS = LOAD 'business.json' USING com.twitter.elephantbird.pig.load.JsonLoader('-nestedLoad') AS jsonmap;
a = FOREACH BUSINESS GENERATE jsonmap#'stars' AS Stars,
    jsonmap#'city' AS City,
    FLATTEN (jsonmap#'categories') AS Categories;

b = FOREACH a GENERATE Categories,City,(double)Stars; 

c = GROUP b by (Categories,City);

d = FOREACH c GENERATE FLATTEN(group) AS (Categories,City),
    AVG(b.Stars) AS Average_Stars_Per_City_Per_Category;

e = ORDER d by Categories, Average_Stars_Per_City_Per_Category DESC;  

STORE e INTO 'output2' USING PigStorage('\t');



   	