package com.yatish.spark.transformations;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class S6_mapToPair {

	public static void main(String[] args) {
		
		SparkConf conf = new SparkConf().setAppName("SparkStart").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		List<Integer> data = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14);	
		JavaRDD<Integer> rddData = sc.parallelize(data);
		
		
		/*
		 * Using 'mapToPair' function, we can convert a list of elements into key value pair.
		 * 
		 * In the below code, we are accepting a list of 'Integer' and returning a key value pair RDD 'JavaPairRDD<Integer,Integer>'.
		 * 'rddData3' value will be --> [(1,1), (2,1), (3,1), (4,1), (5,1), (6,1), (7,1), (8,1), (9,1), (10,1), (11,1), (12,1), (13,1), (14,1)]
		 * 
		 * NOTE: In spark, if want to use key value pair. then we have to use 'Tuple2'
		 */
		
		JavaPairRDD<Integer,Integer> rddData3 = rddData.mapToPair(a -> {
				return new Tuple2<Integer,Integer>(a,1);
		});
		System.out.println(rddData3.collect());
		
		
		/*
		 * Consider, you now have a key value pair RDD, now you want to modify it based on some logic. Now we we cannot alter the key/value like as follows in lambda expression,
		 * 		a._1() = a._1() + 10;		// This will not work, we cannot alter a._1() or a._2() since it is immutable. We have to read the key/value and do some alteration and store in a
		 * 		a._2() = a._2() + 10;		// temporary variable. and then return new 'Tuple2' object with changed data.
		 * 		return a;
		 * 
		 * In the below code will it 'rddData3' value is [(1,1), (2,1), (3,1), (4,1), (5,1), (6,1), (7,1), (8,1), (9,1), (10,1), (11,1), (12,1), (13,1), (14,1)] since are reutrning,
		 * 		return new Tuple2<Integer,Integer>(a._1()+10,a._2()+10);
		 * will the 'rddData5' value will be [(1,1), (2,1), (3,1), (4,1), (5,1), (6,1), (7,1), (8,1), (9,1), (10,1), (11,1), (12,1), (13,1), (14,1),(10,10), (20,10), (30,10), (40,10), (50,10), 
		 * (60,10), (70,10), (80,10), (90,10), (20,10), (21,10), (22,10), (23,10), (24,10)] as we are returning Tuple with different keys, not existing keys, hence the existing data will not be 
		 * changed and the new tuples returned will be added as new entries?
		 * NO!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
		 * Even though we are logically correct, it doesn't work like this. When lambda is executed for each entry, whether the returning tuple is with same key or different key, it doesn't matter.
		 * It will basically replace that entry for which lambda was executed.
		 */
		JavaPairRDD<Integer,Integer> rddData5 = rddData3.mapToPair(a -> {
			//System.out.println(a._1());
			//System.out.println(a._2());
			return new Tuple2<Integer,Integer>(a._1()+10,a._2()+10);
		});
		
		
		
		/*
		 * The same done using lambda above is being done without using lambda in the below code,
		 * 
		 * Spark introduced a new function called 'PairFunction' we have to use this with 'mapToPair' function.
		 * 
		 * In 'PairFunction<Integer,Integer,Integer>', The first 'Integer' refers to the input parameter. Then next set 'Integer,Integer' represent the output type.
		 * NOTE: like how we wrote 'Integer' instead of 'Iterator<Integer>', for key value pairs also we just write 'Integer,Integer' instead of 'Tuple2<Integer,Integer'
		 */
		JavaPairRDD<Integer,Integer> rddData4 = rddData.mapToPair(new PairFunction<Integer,Integer,Integer>() {
			public Tuple2<Integer,Integer> call(Integer in) {
				return new Tuple2<Integer,Integer>(in,1);
			}
		});
		System.out.println(rddData5.collect());

	}

}
