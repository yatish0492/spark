package com.yatish.spark.transformations;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.DoubleFunction;
import org.apache.spark.api.java.function.Function;

public class S2_map {

	public static void main(String[] args) {
		
		SparkConf conf = new SparkConf().setAppName("SparkStart").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		List<Integer> data = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14);	
		JavaRDD<Integer> rddData = sc.parallelize(data);
		
		
		/*
		 * '.map' method is a way to achieve transformation in Spark.
		 * for each element present in the RDD, the transformation function will be executed.
		 * 
		 * NOTE: the transformation Lambda/function should return a value mandatorily. if there is no return done by the lambda/function then the java8 compiler will throw an error.
		 * 			
		 */
		
		/*
		 * In the below code, we are transforming an RDD 'rddData' to 'rddData1'. we are replacing the integers lesser than 5 to -1 and assigning that new RDD to 'rddData1'.
		 * 
		 * We are using Lambda expression to define the functionality of replacing integers lesser than 5 with -1. For each Integer present in 'rddData' the lambda expression will be executed. 
		 * Each integer is being sent as input to the function as 'a'.
		 *	
		 *	Content of 'rddData1' --> [-1, -1, -1, -1, -1, 6, 7, 8, 9, 10, 11, 12, 13, 14]
		 */
		JavaRDD<Integer> rddData1 =	rddData.map(a -> {
											if(a>5) 
												return a;
											else
												return -1;
										});

		
		/*
		 * If you are not good at Lambda, then also no problem at all. you can use anonymous class functions to implement the transformation logic.
		 * 
		 * In the below, code. we are creating an anonymous function, with <Integer,Integer>, the first 'Integer' specifies the input type and the second 'Integer' specifies the output type. We can 
		 * have different output type as well like <Integer,String> etc.
		 * 
		 * We have to implement 'call(<Input_parameter>)' method mandatorily.because, we are creating an anonymous class of 'Function' class, which is an abstract class with abstract method 'call'.
		 * we have to specify the input parameters in the 'call' function. the inputs from the RDD will be sent to these parameters. in this case, each Integer of the RDD 'rddData' is being passed to
		 * 'a' parameter present in the 'call' function.
		 * The transformation logic should be written in 'call' function. The return value from the 'call' function will be returned to the new RDD.
		 * 
		 * Content of 'rddData2' --> [-1, -1, -1, -1, -1, 6, 7, 8, 9, 10, 11, 12, 13, 14]
		 */
		JavaRDD<Integer> rddData2 = rddData.map(new Function<Integer,Integer>() {
			public Integer call(Integer a) {
				if(a>5)
					return a;
				else
					return -1;
			}
		});
		
		
		/*
		 * In spark, we usually will be processing 'double' type values so, spark has provided a specific method called '.mapToDouble'. It always returns the RDD of 'Double' type. spark has 
		 * given new type of RDD 'JavaDoubleRDD' which is same as 'JavaRDD<Double>'. with 'mapToDouble' we need not provide the return type 'Double' in 'mapToDouble' like 
		 * 'mapToDouble<Integer,Double>()'.
		 * 
		 * NOTE: we need to use only 'DoubleFunction' in 'mapToDouble'
		 * 
		 * NOTE: you cannot write 'JavaDoubleRDD<Double>' insted of 'JavaDoubleRDD' evne tough they are same.
		 */
		JavaDoubleRDD rddData3 = rddData.mapToDouble(new DoubleFunction<Integer>() {
			public double call(Integer a) {
				return a.doubleValue();
			}
		});
		
		
		
		
		
		/*
		 * Can we call '.map' on a partitioned RDD?
		 * Yes!!!!
		 * 
		 * I see that '.map' doesn't support input argument as 'Iterator', then how are we going to execute filter on partitioned RDD?
		 * In case of partitioned RDD, of course there is now way we can send the iterator to that partition to the lambda/Function. First partition is taken and the lambda/Function is executed
		 * for each element in it then second partition is taken and the lambda/Function is executed for each element in it so on.
		 */
		

	}

}
