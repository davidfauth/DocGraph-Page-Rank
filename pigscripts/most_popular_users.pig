/*
 * The full Twitter graph data we are using is 1.5B edges in a dataset from 2010,
 * so we take only edges between the top N users. Running pagerank on this
 * subset has the effect of finding "who do influential people think is important".
 */

%default USERNAMES_INPUT_PATH 's3n://medgraph/refer.2011.csv'
%default USERNAMES_INPUT_PATH 's3n://mortar-example-data/twitter-pagerank/twitter_usernames.gz'
--%default EDGES_OUTPUT_PATH 's3n://mortar-example-output-data/$MORTAR_EMAIL_S3_ESCAPED/twitter_influential_user_graph.gz'
--%default USERNAMES_OUTPUT_PATH 's3n://mortar-example-output-data/$MORTAR_EMAIL_S3_ESCAPED/twitter_influential_usernames.gz'
%default EDGES_OUTPUT_PATH 's3n://DataOut/DocGraph/DocEdgesOut'
%default USERNAMES_OUTPUT_PATH 's3n://DataOut/DocGraph/DocTaxonomyOut'%default N 5000

edges                   =   LOAD '$EDGES_INPUT_PATH' USING PigStorage(',') 
                                AS (user: chararray, follower: chararray, qtyReferred:int);
--usernames               =   LOAD '$USERNAMES_INPUT_PATH' USING PigStorage(' ') 
--                               AS (user: int, username: chararray);
nucc_codes = LOAD 's3n://NUCC-Taxonomy/nucc_taxonomy_130.txt' USING PigStorage('\t') AS
(nuccCode:chararray,
nuccType:chararray,
nuccClassification:chararray,
nuccSpecialty:chararray);


-- Load NPI Data
npiData = LOAD 's3n://NPIData/npidata_20050523-20130113.csv' USING PigStorage(',') AS
(NPICode:chararray,
f2:chararray,
f3:chararray,
f4:chararray,
f5:chararray,
f6:chararray,
f7:chararray,
f8:chararray,
f9:chararray,
f10:chararray,
f11:chararray,
f12:chararray,
f13:chararray,
f14:chararray,
f15:chararray,
f16:chararray,
f17:chararray,
f18:chararray,
f19:chararray,
f20:chararray,
f21:chararray,
f22:chararray,
f23:chararray,
f24:chararray,
f25:chararray,
f26:chararray,
f27:chararray,
f28:chararray,
f29:chararray,
f30:chararray,
f31:chararray,
f32:chararray,
f33:chararray,
f34:chararray,
f35:chararray,
f36:chararray,
f37:chararray,
f38:chararray,
f39:chararray,
f40:chararray,
f41:chararray,
f42:chararray,
f43:chararray,
f44:chararray,
f45:chararray,
f46:chararray,
f47:chararray,
f48:chararray,
f49:chararray);

-- build the referred to list 
npiDoctorDetail = foreach npiData generate REPLACE(NPICode,'\\"','') as newNPICode, REPLACE(f7, '\\"', '') as firstName, 
REPLACE(f8, '\\"','') as middleName,
REPLACE(f6,'\\"','') as lastName, 
REPLACE(f5, '\\"','') as orgName,
REPLACE(f31, '\\"','') as docCity,
REPLACE(f32, '\\"','') as docState,
REPLACE(f48, '\\"','') as taxonomyCode;

-- find the users with the most followers
edges_by_user           =   GROUP edges BY user;
users                   =   FOREACH edges_by_user GENERATE 
                                group AS user, 
                                COUNT(edges) AS num_followers;
users_ordered           =   ORDER users BY num_followers DESC;
influential_users       =   LIMIT users_ordered $N;

-- find edges where both the followed user and the following user are influential
edges_jnd_1             =   JOIN edges BY user, influential_users BY user;
followed_is_influential =   FOREACH edges_jnd_1 GENERATE edges::user AS user, edges::follower AS follower;
edges_jnd_2             =   JOIN followed_is_influential BY follower, influential_users BY user;
relevant_edges          =   FOREACH edges_jnd_2 GENERATE
                                followed_is_influential::user AS user,
                                followed_is_influential::follower AS follower;

-- find the usernames of the influential doctors
influential_user_ids    =   FOREACH influential_users GENERATE user;
-- join to get doctors name
influential_doctors_jnd = JOIN npiDoctorDetail by (newNPICode), influential_user_ids by (user);
influential_doctors_taxonomy_jnd = JOIN influential_doctors_jnd by (taxonomyCode), nucc_codes by nuccCode;
--influential_users_jnd   =   JOIN influential_user_ids BY user, usernames BY user;
--influential_usernames   =   FOREACH influential_users_jnd GENERATE
--                                influential_user_ids::user AS user,
--                                usernames::username AS username;
influential_usernames = FOREACH influential_doctors_taxonomy_jnd GENERATE
						newNPICode, lastName;

rmf $EDGES_OUTPUT_PATH;
rmf $USERNAMES_OUTPUT_PATH;
STORE relevant_edges INTO '$EDGES_OUTPUT_PATH' USING PigStorage();
-- Use space delimiter to keep consistent with the input dataset.
STORE influential_usernames INTO '$USERNAMES_OUTPUT_PATH' USING PigStorage(' ');
