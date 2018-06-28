package com.yatish.spark.persistance;

public class S4_Memory_Management {

	/*
	 * Consider, you persist so much of data, the main memory is completely filled and you are trying to persist new RDD, will there be a Out of memory exception?
	 * No!!!
	 * It will automatically remove existing RDD/RDDs based on least-recently-used (LRU) fashion. The least recently used RDD will be removed and new RDD will be saved. We can manually remove an
	 * RDD which is persisted using '.unpersist()' on the corresponding RDD.
	 */
}
