package com.yatish.spark.transformations;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class S7_1_coalesce {

	public static void main(String[] args) {
		
		SparkConf conf = new SparkConf().setAppName("SparkStart").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		List<Integer> data = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14);	
		JavaRDD<Integer> rddData = sc.parallelize(data);
		
		/*
		 * I is same as 'rePartition'. it will also repartition the from existing partitions.
		 * 
		 * If 'coalesce' is same as 'rePartition' then why the hell do we need it?
		 * Actually, 'rePartition' will implicitly shuffle while repartitioning. where as in 'coalesce' you can specify whether to shuffle during repartitioning or not.
		 * 
		 * What is shuffling while repartitioning?
		 * Consider you have 4 partitions as follows,
			 * Partition A : 1,2,3,4
			 * Partition B : 5,6,7,8
			 * Partition C : 9,10,11,12
			 * Partition D : 13,14
		 * Consider we use 'rePartition(2)' to re-partition 4 into 2. Then, it will create a new RDDs with 2 Partitions as follows,
			 * Partition E : 1,3,5,7,9,11,13
			 * Partition F : 2,4,6,8,10,12,14
		 * 		as we can see, 'rePartition' will shuffle the data across partitions like it puts '1' to first partition and '2' to second partition and then '3' to first partition and '4' to first
		 * 		partition etc.
		 * Consider we use 'coalesce(2)' to re-partition 4 into 2. Then, it will create a new RDDs with 2 partitions as follows,
			 * Partition E : 1,2,3,4 + 9,10,11,12
			 * Partition F : 5,6,7,8 + 13,14
		 * 		as we can see, 'coalesce' will not shuffle the data across partitions, it will just copy the 2 partitions A and B to newly created partitions. and then it will simply concat the
		 * 		partitions in sequence, like it will add all the elements of partition C to partition A and add all the elements of partitions D to partition B
		 * 
		 * Can we increase the number of partitions using 'coalesce' without shuffling?
		 * No!!!
		 * you can only decrease the number of partitions without shuffling like from 4 to 2 partitions but you cannot increase the number of partitions without shuffling like from 2 partitions to 4
		 * partitions. Because we know how 'coalesce' works, in this example, of re-partitioning from 2 to 4, it will first try to copy existing partitions to newly created 4 partitions, since the 
		 * existing partitions are not 4 and only 2, it will fail in this this step itself.
		 * 
		 * 
		 * What is 'coalesce' with shuffling?
		 * 'coalesce' with shuffling is same as that of 'rePartition' it will work the same way with no difference at all. 
		 * 
		 */
		
		/*
		 * Spark provides 2 methods to achieve 'coalesce' as follows,
		 * 1) 'coalesce(int NumberOfPartitions)'			---> by default shuffle value is considered as 'false' here.
		 * 2) 'coalesce(int NumberOfPartitions,boolean shuffle)'
		 * 
		 * The below code will not re-partition the RDD into 10 partitions because the number of current partitions is '1'. 'coalesce' without shuffle will not re-partition to a number greater than
		 * the current number of partitions.
		 */
		JavaRDD<Integer> rddData1 = rddData.coalesce(10);
		
		/*
		 * The below code will achieve re-partition which the above code failed to do so. it will create 10 partitions from 1 partition using shuffling. It is same as 'rePartition' without any single
		 * difference.
		 */
		JavaRDD<Integer> rddData2 = rddData.coalesce(10,true);
		
		
		

	}

}
