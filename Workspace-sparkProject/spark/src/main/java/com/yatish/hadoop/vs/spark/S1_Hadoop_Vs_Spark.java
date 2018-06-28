package com.yatish.hadoop.vs.spark;

public class S1_Hadoop_Vs_Spark {

	/*
	 * Hadoop
	 * ------
	 * Hadoop is slower because, The it will write the RDD data to the disk and then read the data from disk so it very slower than spark.
	 * Spark
	 * -----
	 * Spark is faster than Hadoop because, It will not write the RDDs into the disk, it will store them in memory(Main Memory) itself. Ofcouse we can store
	 * it into the disk by setting the persistance level as 
	 * 
	 * 
	 * Hadoop
	 * ------
	 * It is complex to program in hadoop as we have to write a seperate java file for map/mapToPair/filter etc. Basically for every transformation and action, we have to write in a separate
	 * java files which extends the 'Map' class and in driver class, we have to call them.
	 * Spark
	 * -----
	 * Spark is easy to write the transformation by just writing the map function using '.map()' or '.mapToPair()' or '.filter()' instead of writing in seperate java files.
	 * 
	 * 
	 * Hadoop
	 * ------
	 * It doesn't do lazy loading. like for example, we have written a transformation and calling it in driver. The transformations will be executed even though there is no action written.
	 * Spark
	 * -----
	 * It will do lazy loading. like for example, we have written a tarnsformation and no action on the transformed RDD. The transformation will not be executed since there is no action defined.
	 * 
	 * 
	 * Hadoop
	 * ------
	 * It doesn't support streaming. If we want to processess stream data then we have to use other things like apache storm.
	 * Spark
	 * -----
	 * It has built-in support.
	 */
}
