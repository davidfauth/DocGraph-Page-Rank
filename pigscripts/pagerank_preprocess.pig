edges               =   LOAD '$INPUT_PATH' USING PigStorage(',') AS (user: chararray, follower: chararray);
users               =   GROUP edges BY follower;
num_users           =   FOREACH (GROUP users ALL) GENERATE COUNT($1) AS N;

-- copy is to avoid a Pig bug involving storing both
-- an alias and a descendant alias of it too close together
num_users_copy      =   FOREACH num_users GENERATE *;
initial_pageranks   =   FOREACH users GENERATE 
                            group AS user, 
                            1.0 / num_users_copy.N AS pagerank, 
                            edges.user AS following: {t: (user: chararray)};

rmf $NUM_USERS_OUTPUT_PATH;
rmf $PAGERANKS_OUTPUT_PATH;
STORE num_users INTO '$NUM_USERS_OUTPUT_PATH' USING PigStorage();
STORE initial_pageranks INTO '$PAGERANKS_OUTPUT_PATH' USING PigStorage();
