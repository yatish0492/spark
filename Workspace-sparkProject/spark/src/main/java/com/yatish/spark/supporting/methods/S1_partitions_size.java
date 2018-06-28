package com.yatish.spark.supporting.methods;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class S1_partitions_size {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("SparkStart").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		List<Integer> data = Arrays.asList(1, 2, 2, 3, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15);
		JavaRDD<Integer> rddData = sc.parallelize(data,5);
		
		
		/*
		 * Spark provides a method '.partitions' for RDDs. it will return the partitions in for of 'List<Partition>'. we can use collection method '.size()' to find the length of it, hence
		 * we can indirectly 
		 * 
		 */
		System.out.println(rddData.partitions().size());
	}
}
