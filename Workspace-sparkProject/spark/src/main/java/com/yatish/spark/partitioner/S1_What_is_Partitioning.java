package com.yatish.spark.partitioner;

public class S1_What_is_Partitioning {

	/*
	 * We know that in big data, the size of the data is too big such that data cannot be stored in a single node.but we know that we crate RDDs right, so each RDD will sit on each node, then why
	 * do we need this partitioning?
	 * Your understanding is wrong!!!!
	 * RDD(Resilient Distributed Dataset) means this data is distributed across various nodes. a RDD can be so big that we cannot store a RDD in a node. hence we divide a RDD into partitions/slices
	 * each partition/slice sits on different nodes. A RDD is paresent across various nodes not like each RDD sits in each node.
	 * eg: consider a RDD is there say [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14]. if we partition it into 5 partitions, then the result will be
	 * 		partition 1 --> [1,2,3]
	 * 		partition 2 --> [4,5,6]
	 * 		partition 3 --> [7,8,9]
	 * 		partition 4 --> [10,11,12]
	 * 		partition 5 --> [13,14]
	 * 
	 * 
	 * In the answer of above question, we just saw 5 partitions are created. On what basis are the partitions created. On what logic did they divide them?
	 * Spark provides various Partitioners(Binary Partitioner,HashPartitioner, IndexUpdatePartitioner, KeyFieldBasedPartitioner, SleepJob, TotalOrderPartitioner). 
	 * Partitioners are nothing but a class file which holds the logic for how to partition the data.
	 * 
	 * 
	 * How are the partitioner classes implemented in spark?
	 * There is a 'Partitioner' class, all the partitioner classes implements this class.
	 * 
	 * What is the default Partitioner used by spark?
	 * 'HashPartitioner'
	 * 
	 * How does HashPartitioner work?
	 * HashPartitioner works on hashing algorithm. it has a integer property which hold the number of partitions it has to create say 'int numberOfPartitions'. like '.hashCode()' method we have in
	 * java, here also there a method called '.getPartition()' which will return the partition number of the object. By default it will use hashing algorithm which user the memory address of the 
	 * object and gives the partition number based on that.
	 * 
	 * 
	 *	If we are using default Partitioner in spark. it will return based on hashing algorithm. consider i say give me 5 partitions, in this case it has to return the partition number between 1-5
	 *  but hashing algorithm returns something like 27484 or some number between 0-2^8. How do we address this in spark?
	 *  Once it gets the number from hashing algorithm it will do a modulus of the numberOfPartitions. in this case it will do modulus of 5, hence it will get a number between 1-5.
	 *  
	 * 
	 * Why do we need the data to be distributed across various nodes?
	 * 1) It will help manage large amount of data.
	 * 2) It will help in performing parallel processing.
	 * 3) It will provide data loss, if incase a node goes down by keeping replicas.
	 * 
	 */
}
