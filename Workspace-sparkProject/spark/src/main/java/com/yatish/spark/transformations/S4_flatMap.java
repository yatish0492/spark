package com.yatish.spark.transformations;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.DoubleFlatMapFunction;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFlatMapFunction;

import scala.Tuple2;

public class S4_flatMap {

	public static void main(String[] args) {
		
		SparkConf conf = new SparkConf().setAppName("SparkStart").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		List<Integer> data = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14);	
		JavaRDD<Integer> rddData = sc.parallelize(data);
		
		/*
		 * '.flatMap' method is a way to achieve transformation in Spark.
		 * for each element present in the RDD, the transformation function will be executed.
		 * 
		 * NOTE: the transformation Lambda/function should return a Iterator mandatorily. if there is no return of iterator done by the lambda/function then the java8 compiler will throw an error.		
		 */
		
		/*
		 * In the below code, we are transforming an RDD 'rddData' to 'rddData1'. we are replacing the integers lesser than 5 to [15,16,17] and assigning that new RDD to 'rddData1'.
		 * 
		 * Each integer is being sent as input to the function as 'a'.
		 * We are returning an iterator in response.
		 * 
		 *  Content of 'rddData1' --> [15, 16, 17, 15, 16, 17, 15, 16, 17, 15, 16, 17]
		 */
		JavaRDD<Integer> rddData1 = rddData.flatMap(a -> {
			if(a<5) {
				List<Integer> list = Arrays.asList(15,16,17);
				return list.iterator();
			}
			List<Integer> list1 = Arrays.asList();
			return list1.iterator();
		});
		
		
		
		/*
		 * If you are not good at Lambda, then also no problem at all. you can use anonymous class functions to implement the transformation logic.
		 * 
		 * In the below, code. we are creating an anonymous function, with <Integer,Integer>, the first 'Integer' specifies the input type and the second 'Integer' specifies the output Iterator 
		 * type. We can have different output type as well like <Integer,String> etc.
		 * 
		 * We have to implement 'call(<Input_parameter>)' method mandatorily.because, we are creating an anonymous class of 'FlatMapFunction' class, which is an abstract class with abstract method 'call'.
		 * we have to specify the input parameters in the 'call' function. the inputs from the RDD will be sent to these parameters. in this case, each Integer of the RDD 'rddData' is being passed to
		 * 'a' parameter present in the 'call' function.
		 * The transformation logic should be written in 'call' function. The return value from the 'call' function will be returned to the new RDD.
		 * 
		 * Content of 'rddData2' --> [15, 16, 17, 15, 16, 17, 15, 16, 17, 15, 16, 17]
		 * 
		 * NOTE: it is 'FlatMapFunction' not 'Functoin' take care while typing
		 * NOTE: we need not write 'new FlatMapFunction<Integer,Iterator<Integer>>()', we just need to specify just 'Integer' instead of 'Iterator<Integer>' as shown in the code below.
		 * 
		 * Blind Logic: IMPORTANT!!!!!
		 * ---------------------------
		 * The output type(last one) in Function/FlatMapFunction etc should be same as that of the new RDD type. in this case, 'Integer' in 'new FlatMapFunction<Integer,Integer>()' is as in 
		 * 'JavaRDD<Integer>'.
		 * eg: it will be 'new FlatMapFunction<Integer,List<Integer>>()' if the assigning RDD variable type is 'JavaRDD<List<Integer>>'
		 * The input types must exactly match with whatever is mentioned parameters section of 'call' function. In this case, Input is 'Integer' hence call function is 'call(Integer in)'
		 * eg: consider for 'FlatMapPartitions', in 'S6_mapPartions.java', input is 'Iterator<Integer' hence the call function is 'call(Iterator<Integer>)'
		 */
		JavaRDD<Integer> rddData2 = rddData.flatMap(new FlatMapFunction<Integer,Integer>() {
			public Iterator<Integer> call(Integer in) {
				if(in<5) {
					List<Integer> list = Arrays.asList(15,16,17);
					return list.iterator();
				}
				List<Integer> list1 = Arrays.asList();
				return list1.iterator();
			}
		});
		
		
		/*
		 * It is a combination of 'mapToDouble' and 'flatMap' functions. This is a standard method for transforming RDDs into RDD of Double type. The return type is 'JavaDoubleRDD', we cannot mention
		 * 'JavaRDD<Double>' even tough they are same.
		 * 
		 * No need to mention the return type as 'Double' in 'DoubleFlatMapFunction'
		 * 
		 */
		JavaDoubleRDD rddData3 = rddData.flatMapToDouble(new DoubleFlatMapFunction<Integer>() {
			public Iterator<Double> call(Integer in) {
				List<Double> list = new ArrayList<Double>();
				list.add(in.doubleValue());
				return list.iterator();
			}
		});
		
		
		
		/*
		 * 'flatMapToPair' is used to transform an RDD into kay value pair RDD which is referred as 'JavaPairRDD' in spark. we accept an RDD and return a iterator to a list of key value pair.
		 * 
		 * In 'PairFlatMapFunction<Integer,Integer,Integer>', The first 'Integer' refers to the input parameter. Then next set 'Integer,Integer' represent the output type.
		 * NOTE: like how we wrote 'Integer' instead of 'Iterator<Integer>', for key value pairs also we just write 'Integer,Integer' instead of 'Tuple2<Integer,Integer'
		 */
		JavaPairRDD<Integer,Integer> rddData4 = rddData.flatMapToPair(new PairFlatMapFunction<Integer,Integer,Integer>() {
			public Iterator<Tuple2<Integer,Integer>> call(Integer in) {
				List<Tuple2<Integer,Integer>> list = new ArrayList<Tuple2<Integer,Integer>>();
					Tuple2<Integer,Integer> i = new Tuple2<Integer,Integer>(in,1);
					list.add(i);		
				return list.iterator();	
			}
		});
		
		
		
	}

}
