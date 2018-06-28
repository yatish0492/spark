package com.yatish.spark.transformations;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

public class S9_filter {

	public static void main(String[] args) {
		
		SparkConf conf = new SparkConf().setAppName("SparkStart").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		List<Integer> data = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14);	
		JavaRDD<Integer> rddData = sc.parallelize(data);
		
		
		/*
		 * Consider you want to transform an RDD into a RDD but while transforming, you want to filter out few elements based on some condition, in this scenario '.filter' method provided by spark is
		 * very useful.
		 * 
		 * In this case, we are transforming 'rddData' to 'rddData1'. for each element the lambda/Function will be executed, 
		 * if the lambda/Function returns true for that element, then that element will be present in the resulting RDD
		 * if the lambda/Function returns true for that element, then that element will be not be present in the resulting RDD. i.e. that element will be filtered out.
		 * 
		 * 'rddData1' value is --> [1, 2, 3, 4, 5, 6, 7]
		 */
		JavaRDD rddData1 = rddData.filter(a -> {
			System.out.println("lambda called");
			if(a <8) {
				return true;
			}
			return false;
		});
		System.out.println(rddData1.collect());

		
		/*
		 * The same what we implemented using lambda in above code is written using normal java implementation in below code.
		 */
		JavaRDD<Integer> rddData2 = rddData.filter(new Function<Integer,Boolean>() {
			
			public Boolean call(Integer in) {
				if(in <8) {
					return true;
				}
				return false;
			}
		});
		
		
		
		/*
		 * Can we call '.filter' on a partitioned RDD?
		 * Yes!!!!
		 * 
		 * I see that '.filter' doesn't support input argument as 'Iterator', then how are we going to execute filter on partitioned RDD?
		 * In case of partitioned RDD, of course there is now way we can send the iterator to that partition to the lambda/Function. First partition is taken and the lambda/Function is executed
		 * for each element in it then second partition is taken and the lambda/Function is executed for each element in it so on.
		 */
		
		


	}

}
