package com.yatish.spark;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class S6_Spark_Lineage {

	public static void main(String[] args) {
		
		SparkConf conf = new SparkConf().setAppName("SparkStart").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		List<Integer> data = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14);	
		JavaRDD<Integer> rddData = sc.parallelize(data);
		
		JavaRDD<Integer> rddData1 =	rddData.map(a -> {
			System.out.println("Executing the map funtion to create rddData1");
			return a;
		});
		
		/*
		 * What is Spark Lineage?
		 * spark will keep the track of how an RDD is formed like what all transformations are don't get this RDD, this is called as lineage. consider 'rddData1', the lineage of this will be as follow,
		 * 			(1) MapPartitionsRDD[1] at map at S6_Spark_Lineage.java:20 []
 					|  ParallelCollectionRDD[0] at parallelize at S6_Spark_Lineage.java:18 []
 				as we can see above, the lineage  of the RDD 'rddData1' will contain information about 'rddData' parallelization at line 18 and map transformation at line 20.
		 * 
		 * 
		 * How can we see the lineage of an RDD?
		 * Spark provides a method called 'toDebugString' this will return the lineage for that RDD. See the below code for live printing of lineage of 'rddData1'
		 * 
		 */
		System.out.println(rddData1.toDebugString());

	}

}
