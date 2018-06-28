package com.yatish.spark.transformations;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class S2_1_forEach {

	public static void main(String[] args) {
		
		SparkConf conf = new SparkConf().setAppName("SparkStart").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		List<Integer> data = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14);	
		JavaRDD<Integer> rddData = sc.parallelize(data);
		
		/*
		 * 'foreach' is same like 'map', it will also iterate over all the elements in the RDD but 'map' will return a new RDD where as 'foreach' will not return anything. So 'foreach' can be used 
		 *  for performing any actions on the RDD like just printing, saving contents to DB etc. which doesn't create a new set of RDD after execution of the function.
		 *  
		 *  
		 */
		rddData.foreach(a -> {
			System.out.println(a);
		});
		
		sc.close();

	}

}