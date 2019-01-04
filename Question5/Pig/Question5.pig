REGISTER Pig_Jar/json-simple-1.1.1.jar;
REGISTER Pig_Jar/elephant-bird-pig-4.1.jar;
REGISTER Pig_Jar/elephant-bird-core-4.1.jar;
REGISTER Pig_Jar/elephant-bird-hadoop-compat-4.10.jar;

BUSINESS = LOAD 'business.json' USING com.twitter.elephantbird.pig.load.JsonLoader('-nestedLoad') AS jsonmap;
a = FOREACH BUSINESS GENERATE jsonmap#'business_id' AS Business_Id,
    (double)jsonmap#'latitude' AS Latitude,
    (double)jsonmap#'longitude' AS Longitude,
    (double)jsonmap#'stars' AS Stars,
    FLATTEN (jsonmap#'categories') AS Categories;

b = FILTER a BY ACOS(SIN(Latitude*3.142/180)*SIN(55.9469753*3.142/180) + COS(Latitude*3.142/180)*COS(55.9469753*3.142/180)*COS(Longitude*3.142/180 + 3.2096308*3.142/180))*15 <= 15;

c = FOREACH b GENERATE Categories,Business_Id,Stars;

d = FILTER c BY Categories == 'Food';

e = ORDER d BY Stars DESC;

f = LIMIT e 10;

g = ORDER d BY Stars;

h = LIMIT g 10;

i = UNION f,h;

REVIEW = LOAD 'review.json' USING com.twitter.elephantbird.pig.load.JsonLoader('-nestedLoad') AS jsonmap_2;
j = FOREACH REVIEW GENERATE jsonmap_2#'business_id' AS Business_Id,
    (double)jsonmap_2#'stars' AS Stars,
    GetMonth(ToDate(jsonmap_2#'date','yyyy-mm-dd')) AS DATEMONTH;

k = FILTER j BY (DATEMONTH>=1 AND DATEMONTH<=5);

l = JOIN i BY Business_Id,k BY Business_Id;

m = FOREACH l GENERATE $1 AS Business_Id,
    (double)$4 AS Stars;

n = GROUP m BY Business_Id;

o = FOREACH n GENERATE FLATTEN(group) AS Business_Id,
    AVG(m.Stars) AS AVERAGE_NUMBER_OF_STARS;

STORE o INTO 'output5' USING PigStorage('\t');        
    



































   	