# DocGraph PageRank using Mortar

Mortar is a platform-as-a-service for Hadoop.  With Mortar, you can run jobs on Hadoop using Apache Pig and Python without any special training.  You create your project using the Mortar Development Framework, deploy code using the Git revision control system, and Mortar does the rest.

# Getting Started

This Mortar project calculates pageranks for the DocGraph data set.  To run this example:
1. [Obtain an Amazon S3 account](http://aws.amazon.com/s3/)
1. [Signup for a Mortar account](https://app.mortardata.com/signup)
1. [Install the Mortar Development Framework](http://help.mortardata.com/#!/install_mortar_development_framework)
1. Clone this repository to your computer and register it as a project with Mortar:

        git clone git@github.com:davidfauth/DocGraph-Page-Rank.git
        cd DocGraph-Page-Rank
        mortar register DocGraph-Page-Rank

Once everything is set up you can run this example by doing:

        mortar run pagerank --clustersize 5

By default this script will run on the full DocGraph data set with the most followers and finish in about 45 minutes using a 5 node cluster.

# DocGraph Data

The twitter data we're using cames from [About DocGraph](http://notonlydev.com/docgraph/) headed up by [Fred Trotter](http://twitter.com/fredtrotter)

# Pagerank

Pagerank simulates a random walk over a graph where each follower-followed relationship is an edge. The pagerank of a user is the probability that after a large number of steps (starting at a random node) the walk will end up at the user's node. There is also a chance at each step that the walk will "teleport" to a completely random node: this added factor allows the algorithm to function even if there are "attractors" (nodes with no outgoing edges) which would otherwise trap the walk.

Pagerank is an iterative algorithm.  Each pass through the algorithm relies on the previous pass' output pageranks (or in the case of the first pass a set of default pageranks generated for each node).  The algorithm is considered done when a new pass through the data produces results that are "close enough" to the previous pass.  See http://en.wikipedia.org/wiki/PageRank for a more detailed algorithm explanation.

# What's inside

## Control Script

The file ./controlscripts/pagerank.py is the top level script that we're going to run in Mortar.  Using [Embedded Pig](http://help.mortardata.com/reference/pig/embedded_pig) this Jython code is responsible for running our various pig scripts in the correct order and with the correct parameters.

For easier debugging of control scripts all print statements are included in the pig logs shown on the job details page in the Mortar web application.

The control scripts contain references to data stored on your personal Amazon S3 buckets. These will need to be changed for the job to run correctly. These are located in the #Input Data Paths section.

## Pig Scripts

This project contains four pig scripts:

### most\_popular\_users.pig

This pig script takes the full DocGaph graph and returns the subset of the graph that includes only the top 750 doctors/hospitals/labs. 

### pagerank\_preprocess.pig

This pig script takes our input data and converts it into the format that we'll use for running the iterative pagerank algorithm.  This script is also responsible for setting the starting pagerank values for each user.

### pagerank\_iterate.pig

This pig script calculates updated pagerank values for each user in the DocGraph graph.  It takes as input the previous pagerank values calculated for each user.  This script also calculates a 'max\_diff' value that is the largest change in pagerank for any user in the graph.  This value is used by the control script to determine if its worth running another iteration to calculate even more accurate pagerank values.

### pagerank\_postprocess.pig

This pig script takes the final pagerank values calculated for each user and writes the top 750 users and their pageranks to S3.

# Pagerank Parameters

## Damping Factor

The damping factor determines the variance of the final output pageranks.  This is a number between 0 and 1 where (1 - DAMPING\_FACTOR) is the probability of the random walk teleporting to a random node in the graph. At 0 every node would have the same pagerank (since edges would never be followed).  Setting it to 1 would mean the walks get trapped by attractor nodes and would rarely visit nodes with no incoming edges.  A common value for the damping factor is 0.85.

## Convergence Threshold

Pagerank is an iterative algorithm where each run uses the previous run's results.  It stops when the maximum difference of a user's pagerank from one iteration to the next is less than the CONVERGENCE\_FACTOR.
