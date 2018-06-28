package com.yatish.spark.transformations;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;

import scala.Tuple2;

public class S17_Joins {

	public static void main(String[] args) {
		
		SparkConf conf = new SparkConf().setAppName("SparkStart").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		List<Integer> data = Arrays.asList(1, 2);	
		JavaRDD<Integer> rddData = sc.parallelize(data);
		
		List<Integer> data1 = Arrays.asList(1, 3);	
		JavaRDD<Integer> rddData1 = sc.parallelize(data1);
		
		JavaPairRDD<Integer,Integer> rddData2 = rddData.mapToPair(a -> {
			return new Tuple2<Integer,Integer>(a,1);
		});
		
		JavaPairRDD<Integer,Integer> rddData3 = rddData1.mapToPair(a -> {
			return new Tuple2<Integer,Integer>(a,2);
		});
		
		/*
		 * rddData2 --> [(1,1), (2,1)]
		 * rddData3 --> [(1,2), (3,2)]
		 */
		
		/*
		 * joinedRDD --> [(1,(1,2))]
		 * 
		 * It is like LeftOuter Join
		 */
		JavaPairRDD<Integer,Tuple2<Integer,Integer>> joinedRDD = rddData2.join(rddData3);
		
		/*
		 * fullOuterJoinedRDD --> [(1,(Optional[1],Optional[2])), (3,(Optional.empty,Optional[2])), (2,(Optional[1],Optional.empty))]
		 * 
		 * NOTE : return type is not JavaPairRDD<Integer,Tuple2<Integer,Integer>>, it is JavaPairRDD<Integer,Tuple2<Optional<Integer>,Optional<Integer>>>
		 */
		JavaPairRDD<Integer,Tuple2<Optional<Integer>,Optional<Integer>>> fullOuterJoinedRDD = rddData2.fullOuterJoin(rddData3);
		
		/*
		 * leftOuterJoinRDD --> [(1,(1,Optional[2])), (2,(1,Optional.empty))]
		 * 
		 * NOTE : return type is not JavaPairRDD<Integer,Tuple2<Optional<Integer>,Optional<Integer>>>, it is JavaPairRDD<Integer,Tuple2<Integer,Optional<Integer>>>. Only 2nd Integer in Tuple is 
		 * 			optional
		 */
		JavaPairRDD<Integer,Tuple2<Integer,Optional<Integer>>> leftOuterJoinRDD = rddData2.leftOuterJoin(rddData3);
		
		
		/*
		 * rightOuterJoinRDD --> [(1,(Optional[1],2)), (3,(Optional.empty,2))]
		 * 
		 * NOTE : return type is not JavaPairRDD<Integer,Tuple2<Optional<Integer>,Optional<Integer>>>, it is JavaPairRDD<Integer,Tuple2<Optional<Integer>,Integer>>. Only 1st Integer in Tuple is 
		 * 			optional
		 */
		JavaPairRDD<Integer,Tuple2<Optional<Integer>,Integer>> rightOuterJoinRDD = rddData2.rightOuterJoin(rddData3);
		
		
		
		System.out.println(rddData2.collect());
		System.out.println(rddData3.collect());
		
		

	}

}
