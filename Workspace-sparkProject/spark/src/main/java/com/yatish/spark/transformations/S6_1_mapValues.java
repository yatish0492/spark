package com.yatish.spark.transformations;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class S6_1_mapValues {

	public static void main(String[] args) {
		
		SparkConf conf = new SparkConf().setAppName("SparkStart").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		List<Integer> data = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14);	
		JavaRDD<Integer> rddData = sc.parallelize(data);
		
		JavaPairRDD<Integer,Integer> rddData3 = rddData.mapToPair(a -> {
			return new Tuple2<Integer,Integer>(a,1);
		});
		
		/*
		 * We know that in a map we will not or should not change the key value once inserted. Hence, spark provides one more method '.mapValues' which will execute the lambda for each value element.
		 * In this case, lambda 'a' will be value of each entry not complete 'Tuple'.
		 */
		JavaPairRDD<Integer,Integer> rddData5 = rddData3.mapValues(a -> {
			a = a + 10; // or Integer b = a + 10;
			return a;   // or return b;
		});

	}

}
