-- Adapted from Alan Gates' Programming Pig - http://ofps.oreilly.com/titles/9781449302641/embedding.html

previous_pageranks      =   LOAD '$INPUT_PATH' USING PigStorage()
                            AS (user: chararray, pagerank: double, following: {t: (user: chararray)});

outbound_pageranks      =   FOREACH previous_pageranks GENERATE
                                FLATTEN(following) AS user: chararray,
                                pagerank / COUNT(following) AS pagerank: double;

cogrouped               =   COGROUP previous_pageranks BY user, outbound_pageranks BY user;
new_pageranks           =   FOREACH cogrouped GENERATE
                                group AS user,
                                ((1.0 - $DAMPING_FACTOR) / $NUM_USERS)
                                    + $DAMPING_FACTOR * SUM(outbound_pageranks.pagerank) AS pagerank,
                                FLATTEN(previous_pageranks.following) AS following,
                                FLATTEN(previous_pageranks.pagerank) AS previous_pagerank;

no_nulls                =   FILTER new_pageranks BY pagerank is not null AND previous_pagerank is not null;
pagerank_diffs          =   FOREACH no_nulls GENERATE ABS(pagerank - previous_pagerank);
max_diff                =   FOREACH (GROUP pagerank_diffs ALL) GENERATE MAX($1);

rmf $PAGERANKS_OUTPUT_PATH;
rmf $MAX_DIFF_OUTPUT_PATH;
STORE new_pageranks INTO '$PAGERANKS_OUTPUT_PATH' USING PigStorage();
STORE max_diff INTO '$MAX_DIFF_OUTPUT_PATH' USING PigStorage();
