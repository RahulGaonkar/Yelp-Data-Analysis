REGISTER Pig_Jar/json-simple-1.1.1.jar;
REGISTER Pig_Jar/elephant-bird-pig-4.1.jar;
REGISTER Pig_Jar/elephant-bird-core-4.1.jar;
REGISTER Pig_Jar/elephant-bird-hadoop-compat-4.10.jar;

BUSINESS = LOAD 'business.json' USING com.twitter.elephantbird.pig.load.JsonLoader('-nestedLoad') AS jsonmap;
a = FOREACH BUSINESS GENERATE (double)jsonmap#'stars' AS Stars,
    jsonmap#'business_id' AS Business_ID,
    (double)jsonmap#'latitude' AS Latitude,
    (double)jsonmap#'longitude' AS Longitude,
    jsonmap#'name' AS Name,
    FLATTEN (jsonmap#'categories') AS Categories;

b = FILTER a BY ACOS(SIN(Latitude*3.142/180)*SIN(55.9469753*3.142/180) + COS(Latitude*3.142/180)*COS(55.9469753*3.142/180)*COS(Longitude*3.142/180 + 3.2096308*3.142/180))*15 <= 15;

c = FOREACH b GENERATE Categories,Business_ID,Name,Stars;

d = ORDER c BY Categories,Stars DESC;  
  
STORE d INTO 'output3' USING PigStorage('\t');



   	