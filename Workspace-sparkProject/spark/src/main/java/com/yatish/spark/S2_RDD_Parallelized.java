package com.yatish.spark;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class S2_RDD_Parallelized {

	public static void main(String[] args) {
		
		SparkConf conf = new SparkConf().setAppName("SparkStart").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		
		/*
		 * Parallelized RDD means, we have the data for creating RDDs within the driver program only
		 * instead of referencing to a data from any file system like HDFS or anything
		 * 
		 */
		
		
		List<Integer> data = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14);
		/*
		 * In the below code, we are using the data assigned in above statement, which is within this
		 * driver program only. so spark provides an API called 'parallelize(data)' to create RDDs 
		 * from the data present within the driver program
		 * 
		 */
		JavaRDD<Integer> distData = sc.parallelize(data);
		
		
		/*
		 * The number of RDDs or slices to be created out of the data is decided by spark only.
		 * User can also specifically tell, the number of slices to be done from the data
		 * 
		 */
		JavaRDD<Integer> distData1 = sc.parallelize(data,20);
		System.out.println("");
		
		/*
		 * There are other 'parallelize()' support as well as follows,
		 * 1) sc.parallelizeDoubles(List<Double>)	--> This will create RDDs of only list of Double Type.
		 * 2) sc.parallelizeDoubles(List<Double>,int NumberOfSlices)	--> This will create RDDs of only list of Double Type.
		 * 3) sc.parallelizePairs(List<Tuple2<Key_Type,Value_Type>>)	--> This will create RDDs of only list of Tupple2 type.
		 * 4) sc.parallelizePairs(List<Tuple2<Key_Type,Value_Type>>,int NumberOfSlices)	--> This will create RDDs of only list of Tupple2 type.
		 * 
		 * NOTE: sc.parallelize(List<Any_Type>)	--> this will accept list of any type
		 */
		
		JavaPairRDD<Integer,String> abc = distData1.mapToPair(new PairFunction<Integer,Integer,String>() {
			public Tuple2<Integer,String> call(Integer in) {
				return new Tuple2(in,"");
			}
		}
		);
		JavaRDD<Integer> abb = distData1.map(a -> a);
		JavaRDD<Integer> def = distData1.flatMap(new FlatMapFunction<Integer,Integer>() {
			public Iterator<Integer> call(Integer in) {
				System.out.println(data.size());
				return data.iterator();
			}
		});
	
		System.out.println("FlatMapFunction --> " + def.collect());
		System.out.println(distData1.reduce((a,b) -> a + b));
		
		sc.close();

	}

}
