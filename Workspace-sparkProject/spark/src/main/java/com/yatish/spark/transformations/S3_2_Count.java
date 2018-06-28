package com.yatish.spark.transformations;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class S3_2_Count {

	public static void main(String[] args) {
		
		SparkConf conf = new SparkConf().setAppName("SparkStart").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		List<Integer> data = Arrays.asList(10, 11, 12, 13, 14, 1, 2, 3, 4, 5, 6, 7, 8, 9);	
		JavaRDD<Integer> rddData = sc.parallelize(data);
		
		/*
		 * Consider you want to know about how many elements are there in different name nodes distributed as RDD, In that case, spark provides one more method 'count()' which returns this count.
		 */
		System.out.println(rddData.count());

	}

}
