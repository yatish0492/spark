package com.yatish.spark.transformations;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaFutureAction;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class S3_3_Async {

	public static void main(String[] args) {
		
		SparkConf conf = new SparkConf().setAppName("SparkStart").setMaster("local").set("spark.scheduler.mode", "FAIR");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		List<Integer> data = Arrays.asList(10, 11, 12, 13, 14, 1, 2, 3, 4, 5, 6, 7, 8, 9);	
		JavaRDD<Integer> rddData = sc.parallelize(data);
		
		/*
		 * The below code will execute '.foreach' first completely on the rdd and then only '.count()' will be executed. Spark executes the operations on an RDD synchrnously hence the actions on 
		 * RDD will be executed sequentially only one after the other. If you want to execute asynchronously then refer to the next comment.
		 */
		rddData.foreach(a -> {
			System.out.println(a);
		});
		System.out.println(rddData.count());
		
		
		/*
		 * If you want to execute rdd actions asynchronously. then you have to use asynchronous methods 'countAsync' and 'takeAsync'. In the below code, spark will call 'foreachAsync' and then 
		 * doesn't wait there till that is executed fully. It will just call it and then go and call 'countAsync'.
		 * 
		 *  But we have one more problem here, even though the tasks will be executed aynchronously, by default the spark job scheduler uses FIFO to execute the jobs hence consider it starts executing
		 *  'forCountAsync' until this task is not completed the other asynchronous tasks will not be executed one after the other as per FIFO order. You have to set the 'spark.scheduler.mode' to 'FAIR'
		 *  in 'sparkConf()' as showing in the java file so that the tasks will not be executed in FIFO but as per FAIR way.
		 *  
		 */
		rddData.foreachAsync(a -> {
			System.out.println(a);
		});
		try {
			System.out.println(rddData.countAsync().get());
		} catch(Exception e) {}
		
		
		JavaFutureAction<Long> intCount = rddData.countAsync();
		
		
		JavaFutureAction<List<Integer>> takeAsync = rddData.takeAsync(4);
		
		rddData.foreachAsync(a -> {
			System.out.println(a);
		});
		
	}

}
