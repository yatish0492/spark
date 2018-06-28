package com.yatish.spark.transformations;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class S3_5_CountByValue {

	public static void main(String[] args) {
		
		SparkConf conf = new SparkConf().setAppName("SparkStart").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		List<Integer> data = Arrays.asList(1, 2, 3, 4, 5, 6, 6, 7, 8, 8, 9, 9, 10);	
		JavaRDD<Integer> rddData = sc.parallelize(data);
		
		/*
		 * '.countByValue()' will return us a map of element and the corresponding count of it. 
		 */
		Map<Integer,Long> wordCount = rddData.countByValue();  // Output - {5=1, 10=1, 1=1, 6=2, 9=2, 2=1, 7=1, 3=1, 8=2, 4=1}

	}

}
