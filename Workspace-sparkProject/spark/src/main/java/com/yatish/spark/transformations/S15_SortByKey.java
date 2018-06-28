package com.yatish.spark.transformations;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class S15_SortByKey {

	public static void main(String[] args) {
		
		SparkConf conf = new SparkConf().setAppName("SparkStart").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		List<Integer> data = Arrays.asList(10, 11, 12, 13, 14, 1, 2, 3, 4, 5, 6, 7, 8, 9);	
		JavaRDD<Integer> rddData = sc.parallelize(data);
		
		JavaPairRDD<Integer,Integer> rddData3 = rddData.mapToPair(a -> {
			return new Tuple2<Integer,Integer>(a,1);
		});
		
		/*
		 * This will sort the Tuples based on the key value. Once method type is '.sortByKey(boolean assendingOrNot)'. if value of 'assendingOrNot' is true, then the ordering will be done in ascending 
		 * order.
		 */
		rddData3.sortByKey(true);
		
		
		/*
		 * There is one more method type '.sortByKey(comparatorFunction)'. Based on comparator it will sort.
		 * 
		 * NOTE: we cannot return boolean type, we have to return,
		 * 		positive integer if 'a' is greater than 'b'. 
		 * 		negative integer if 'a' is lesser than 'b'. 
		 * 		0, if equal.
		 */
		rddData3.sortByKey((a,b) -> {
			return a - b;
		});
		

	}

}
