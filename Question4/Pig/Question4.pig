REGISTER Pig_Jar/json-simple-1.1.1.jar;
REGISTER Pig_Jar/elephant-bird-pig-4.1.jar;
REGISTER Pig_Jar/elephant-bird-core-4.1.jar;
REGISTER Pig_Jar/elephant-bird-hadoop-compat-4.10.jar;

BUSINESS = LOAD 'business.json' USING com.twitter.elephantbird.pig.load.JsonLoader('-nestedLoad') AS jsonmap;
a = FOREACH BUSINESS GENERATE jsonmap#'business_id' AS Business_Id,
    (double)jsonmap#'latitude' AS Latitude,
    (double)jsonmap#'longitude' AS Longitude,
    FLATTEN (jsonmap#'categories') AS Categories;

b = FILTER a BY ACOS(SIN(Latitude*3.142/180)*SIN(55.9469753*3.142/180) + COS(Latitude*3.142/180)*COS(55.9469753*3.142/180)*COS(Longitude*3.142/180 + 3.2096308*3.142/180))*15 <= 15;

c = FOREACH b GENERATE Categories,Business_Id;

REVIEW = LOAD 'review.json' USING com.twitter.elephantbird.pig.load.JsonLoader('-nestedLoad') AS jsonmap_2;
d = FOREACH REVIEW GENERATE jsonmap_2#'business_id' AS Business_Id,
    jsonmap_2#'user_id' AS User_Id,
    jsonmap_2#'stars' AS Stars;

e = JOIN c by Business_Id,d by Business_Id;

f = FOREACH e GENERATE $3 AS User_Id;
 
g = GROUP f by (User_Id);

h = FOREACH g GENERATE FLATTEN(group) AS (User_Id),
    COUNT(f.User_Id) AS NUMBER_OF_REVIEWS_PER_USER;

i = ORDER h BY NUMBER_OF_REVIEWS_PER_USER DESC;

j = LIMIT i 10;

k = JOIN j BY User_Id,e BY d::User_Id;  

USER = LOAD 'user.json' USING com.twitter.elephantbird.pig.load.JsonLoader('-nestedLoad') AS jsonmap_3;
l = FOREACH USER GENERATE jsonmap_3#'user_id' AS User_Id,
    jsonmap_3#'name' AS Name;

m = JOIN k by j::User_Id,
    l by User_Id;

n = FOREACH m GENERATE  k::e::c::Categories AS Categories,
    k::j::User_Id AS User_Id,
    l::Name AS Name,
    (double)k::e::d::Stars AS Stars;

o = GROUP n BY (Categories,User_Id);

p = FOREACH o GENERATE FLATTEN(group) AS (Categories,User_Id),
    n.Name AS Name,
    AVG(n.Stars) AS AVERAGE_NUMBER_OF_STARS;

STORE p INTO 'output4' USING PigStorage('\t');  
 



   	