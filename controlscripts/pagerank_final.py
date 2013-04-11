from org.apache.pig.scripting import Pig

# Pagerank Parameters - See README.md for more information.
DAMPING_FACTOR        = 0.85
CONVERGENCE_THRESHOLD = 0.00004

# Script Parameters
NUM_TOP_USERS      = 1000 # The number of users with with the highest pagerank returned in the final result.
MAX_NUM_ITERATIONS = 5    # The max number of iterations to run.

# Pigscript paths
PREPROCESS_SCRIPT       = "../pigscripts/pagerank_preprocess.pig"
PAGERANK_ITERATE_SCRIPT = "../pigscripts/pagerank_iterate.pig"
POSTPROCESS_SCRIPT      = "../pigscripts/pagerank_postprocess.pig"

# Input Data Paths
--FOLLOWER_GRAPH_INPUT = "s3n://mortar-example-data/twitter-pagerank/twitter_influential_user_graph/*"
FOLLOWER_GRAPH_INPUT = "s3n://medgraph/refer.2011.csv"
USERNAMES_INPUT      = "s3n://mortar-example-data/twitter-pagerank/twitter_usernames.gz"

# Temporary Data Paths - Use HDFS for better performance
HDFS_OUTPUT_PREFIX         = "hdfs:///docGraph_pagerank"
PREPROCESS_PAGERANKS       = HDFS_OUTPUT_PREFIX + "/preprocess/pageranks"
PREPROCESS_NUM_USERS       = HDFS_OUTPUT_PREFIX + "/preprocess/num_users"
ITERATION_PAGERANKS_PREFIX = HDFS_OUTPUT_PREFIX + "/iteration/pageranks_"
ITERATION_MAX_DIFF_PREFIX  = HDFS_OUTPUT_PREFIX + "/iteration/max_diff_"

# Output Data Paths - Full path is defined in the pigscript in order to access the
# user specific email parameter and generate unique output paths.
OUTPUT_BUCKET = "mortar-example-output-data"


def run_pagerank():
    """
    Calculates pageranks for Twitter users.

    Three main steps:
        1. Preprocessing: Process input data to:
             a) Count the total number of users.
             b) Prepare initial pagerank values for all users.
        2. Iterative: Calculate new pageranks for each user based on the previous pageranks of the
                      users' followers.
        3. Postprocesing: Find the top pagerank users and join to a separate dataset to find their names.
    """
    # Preprocessing step:
    print "Starting preprocessing step."
    preprocess = Pig.compileFromFile(PREPROCESS_SCRIPT)
    preprocess_bound = preprocess.bind({
        "INPUT_PATH": FOLLOWER_GRAPH_INPUT,
        "PAGERANKS_OUTPUT_PATH": PREPROCESS_PAGERANKS,
        "NUM_USERS_OUTPUT_PATH": PREPROCESS_NUM_USERS
    })
    preprocess_stats = preprocess_bound.runSingle()
    num_users = int(str(preprocess_stats.result("num_users").iterator().next().get(0)))
    convergence_threshold = CONVERGENCE_THRESHOLD / num_users


    # Iteration step:
    iteration = Pig.compileFromFile(PAGERANK_ITERATE_SCRIPT)
    for i in range(MAX_NUM_ITERATIONS):
        print "Starting iteration step: %s" % str(i + 1)

        # Append the iteration number to the input/output stems
        iteration_input = PREPROCESS_PAGERANKS if i == 0 else (ITERATION_PAGERANKS_PREFIX + str(i-1))
        iteration_pageranks_output = ITERATION_PAGERANKS_PREFIX + str(i)
        iteration_max_diff_output = ITERATION_MAX_DIFF_PREFIX + str(i)

        iteration_bound = iteration.bind({
            "INPUT_PATH": iteration_input,
            "DAMPING_FACTOR": DAMPING_FACTOR,
            "NUM_USERS": num_users,
            "PAGERANKS_OUTPUT_PATH": iteration_pageranks_output,
            "MAX_DIFF_OUTPUT_PATH": iteration_max_diff_output
        })
        iteration_stats = iteration_bound.runSingle()

        # If we're below the convergence_threshold break out of the loop.
        max_diff = float(str(iteration_stats.result("max_diff").iterator().next().get(0)))
        if max_diff < CONVERGENCE_THRESHOLD:
            print "Max diff %s under convergence threshold. Stopping." % max_diff
            break
        elif i == MAX_NUM_ITERATIONS-1:
            print "Max diff %s above convergence threshold but hit max number of iterations.  Stopping." \
                    % max_diff
        else:
            print "Max diff %s above convergence threshold. Continuing." % max_diff

    iteration_pagerank_result = ITERATION_PAGERANKS_PREFIX + str(i)

    # Postprocesing step:
    print "Starting postprocessing step."
    postprocess = Pig.compileFromFile(POSTPROCESS_SCRIPT)
    postprocess_bound = postprocess.bind({
        "PAGERANKS_INPUT_PATH": iteration_pagerank_result,
        "USERNAMES_INPUT_PATH": USERNAMES_INPUT,
        "TOP_N": NUM_TOP_USERS,
        "OUTPUT_BUCKET": OUTPUT_BUCKET
    })
    postprocess_stats = postprocess_bound.runSingle()

if __name__ == "__main__":
    run_pagerank()
