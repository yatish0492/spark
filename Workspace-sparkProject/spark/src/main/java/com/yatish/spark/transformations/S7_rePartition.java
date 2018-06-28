package com.yatish.spark.transformations;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class S7_rePartition {

	public static void main(String[] args) {
		
		SparkConf conf = new SparkConf().setAppName("SparkStart").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		List<Integer> data = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14);	
		JavaRDD<Integer> rddData = sc.parallelize(data);
		
		/*
		 * consider, you have an RDD created without any partitioning like how we have created 'rddData' and now want that RDD to be partitioned or consider you have an RDD already partitioned with
		 * some value say 5 and now you need to partition it into 10 slices. How can i do that?
		 * This is where 'rePartition' method provided by spark comes into picture. using this method we can do it as follows in the code. 'rddData1' is created with 10 partitions/slices.
		 */
		
		JavaRDD<Integer> rddData1 = rddData.repartition(10);
		JavaRDD<Integer> rddData2 = rddData1.repartition(15);
		

	}

}
