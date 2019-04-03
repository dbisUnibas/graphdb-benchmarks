# Graph DB Benchmarking Client

The project graphdb-benchmarks is a benchmark between popular graph dataases. Currently the framework supports [OrientDB](http://www.orientechnologies.com/orientdb/), [Neo4j](http://neo4j.com/) and [Sparksee](http://www.sparsity-technologies.com/). The purpose of this benchmark is to examine the performance of each graph database in terms of execution time. The benchmark is composed of four workloads, Clustering, Massive Insertion, Single Insertion and Query Workload. Every workload has been designed to simulate common operations in graph database systems.

- *Clustering Workload (CW)*: CW consists of a well-known community detection algorithm for modularity optimization, the Louvain Method. We adapt the algorithm on top of the benchmarked graph databases and employ cache techniques to take advantage of both graph database capabilities and in-memory execution speed. We measure the time the algorithm needs to converge.
- *Massive Insertion Workload (MIW)*: we create the graph database and configure it for massive loading, then we populate it with a particular dataset. We measure the time for the creation of the whole graph.
- *Single Insertion Workload (SIW)*: we create the graph database and load it with a particular dataset. Every object insertion (node or edge) is committed directly and the graph is constructed incrementally. We measure the insertion time per block, which consists of one thousand edges and the nodes that appear during the insertion of these edges.
- *Query Workload (QW)*: we execute three common queries:
  * FindNeighbours (FN): finds the neighbours of all nodes.
  * FindAdjacentNodes (FA): finds the adjacent nodes of all edges.
  * FindShortestPath (FS): finds the shortest path between the first node and 100 randomly picked nodes.

Here we measure the execution time of each query.

For our evaluation we use both synthetic and real data. More specifically, we execute MIW, SIW and QW with real data derived from the SNAP dataset collection ([Enron Dataset](http://snap.stanford.edu/data/email-Enron.html), [Amazon dataset](http://snap.stanford.edu/data/amazon0601.html), [Youtube dataset](http://snap.stanford.edu/data/com-Youtube.html) and [LiveJournal dataset](http://snap.stanford.edu/data/com-LiveJournal.html)). On the other hand, with the CW we use synthetic data generated with the [LFR-Benchmark generator](https://sites.google.com/site/andrealancichinetti/files) which produces networks with power-law degree distribution and implanted communities within the network. The synthetic data can be downloaded form [here](http://figshare.com/articles/Synthetic_Data_for_graphdb_benchmark/1221760).



## Instructions
To run the project at first you have to choose one of the aforementioned datasets. Of course you can select any dataset, but because there is not any utility class to convert the dataset in the appropriate format (for now), the format of the data must be identical with the tested datasets. The input parameters are configured from the src/test/resources/input.properties file. Please follow the instructions in this file to select the correct parameters. Then, run `mvn dependency:copy-dependencies && mvn test -Pbench` to execute the benchmarking run.

