package com.yatish.spark.transformations;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class S6_2_filter_on_pairRDD {

	public static void main(String[] args) {
		
		SparkConf conf = new SparkConf().setAppName("SparkStart").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		List<Integer> data = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14);	
		JavaRDD<Integer> rddData = sc.parallelize(data);
		
		JavaPairRDD<Integer,Integer> rddData3 = rddData.mapToPair(a -> {
			return new Tuple2<Integer,Integer>(a,1);
		});
		
		/*
		 * We can perform '.filter' on 'pairRDD'. the value of 'a' in lambda will be assigned with each complete 'Tuple' not just the value of each tuple.
		 */
		JavaPairRDD<Integer,Integer> rddData4 = rddData3.filter(a -> {
			if(a._1() > 5) {
				return true;
			} else {
				return false;
			}
			
		});

	}

}
