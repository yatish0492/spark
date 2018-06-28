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
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class S8_mapPartitions {

	public static void main(String[] args) {
		
		SparkConf conf = new SparkConf().setAppName("SparkStart").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		List<Integer> data = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14);	
		JavaRDD<Integer> rddData = sc.parallelize(data,4);
		
		/*
		 * '.flatMap' method is a way to achieve transformation in Spark.
		 * for each partition present in the RDD, the transformation function will be executed.
		 * 
		 * NOTE: the transformation Lambda/function should return a Iterator mandatorily. if there is no return of iterator done by the lambda/function then the java8 compiler will throw an error.		
		 */
		
		/*
		 * In the below code, we are transforming an RDD 'rddData' to 'rddData1'. we are just returning the partitions(iterator), which are recieved as input from 'rddData' to 'rddData1'.
		 * 
		 * Each partition(iterator) is being sent as input to the function as 'a'.
		 * We are returning an iterator in response. we cannot return a value since we are processing and returning a partition(slice).
		 * 
		 * In this case, the lambda will be executed 4 times for each partion(slice) because we have mentioned 'numOfSlices' as 4 in 'avaRDD<Integer> rddData = sc.parallelize(data,4);'
		 * 
		 *  Content of 'rddData1' --> [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14]
		 */
		JavaRDD<Integer> rddData1 = rddData.mapPartitions(a -> {
			System.out.println(" lambda called");
			List<Integer> list = new ArrayList<Integer>();
			while(a.hasNext()) {
				Integer i = a.next();
				list.add(i);	
			}	
			return list.iterator();	
		});
		
		
		/*
		 * This just normal function implmentation instead of lambda.
		 * 
		 * Here we have mentioned the input types in 'FlatMapFunction' as 'Iterator<Integer>' instead of 'Integer' why?
		 * Remember the Blind logic discussed in 's4_flatMap.java'. Even if we are returning an Iterator, in 'FlatMapFunction' we just specify the type of iterator 'Integer' instead of 
		 * 'Interator<Integer>'. But for Input types, we have specify exactly what is being sent as input like we have to specify 'Iterator<Integer>' only not just 'Integer.
		 * Spark is doing Partiality between Input and Output Types in 'FlatMapFunction' Hahaha!!!!
		 */
		JavaRDD<Integer> rddData2 = rddData.mapPartitions(new FlatMapFunction<Iterator<Integer>,Integer>() {
			
			public Iterator<Integer> call(Iterator<Integer> itr) {
				System.out.println(" called in FlatMapFunction 1");
				List<Integer> list = new ArrayList<Integer>();
				while(itr.hasNext()) {
					Integer i = itr.next();
					System.out.println(i);
					list.add(i + 100);	
				}	
				return list.iterator();	
			}
		});
		

		/*
		 * When JVM executes below code, at that time only it will execute the lambda and assign the value to 'rddData1'. It won't execute the 'FlatMapFunction' and assign the value to 'rddData2'
		 * after executing lambda because, there is no action called on 'rddData1'
		 */
		System.out.println(rddData1.collect());
		
		
		/*
		 * When JVM executes below code, at that time only it will execute the 'FlatMapFunction' and assign the value to 'rddData2'.
		 */
		System.out.println(rddData2.collect());
		
		
		/*
		 * Consider, instead of transforming 'rddData' to 'rddData2', If i transform 'rddData1' to 'rddData2' i.e. 'JavaRDD<Integer> rddData2 = rddData1.mapPartitions(...' and i have a action
		 * statement here for 'rddData2' say 'System.out.println(rddData2.collect());' What will happen?
		 * It will execute both the RDDs!!!
		 * Yes, it will because we called an action on 'rddData2' but since  it is dependent on 'rddData1'. Spark will execute the 'rddData1' first and then it will execute the 'rddData2'
		 */
		
		
		
		/*
		 * '.mapPartitions' method supports one more boolean parameter called 'preservesPartitioning'.
		 * 	In below code, we have passed 'true' after the function, which is argument for 'preservesPartitioning' parameter.
		 * 
		 * What does 'preservePartitioning' parameter do?
		 * During transformation process, it is not garuenteed that the partitions will remain same as before, after the transformation process. Especially when working with paired RDDs like Tupples.
		 * RDDs do preserve a metadata as well along with the data. That meta data contains information about which element belongs to which partition, in some transformations this partitioning 
		 * meta data will not be shared with the destination RDD(rddData3). Hence if we set this parameter as true, then the partitioning meta data information will be shared to the destination RDD.
		 * Hence the partitioning information will not be lost. If false it won't share that partitioning information.
		 * 
		 * NOTE: 'preservesPartitioning' parameter is be provided for other partitioning methods mentioned in this file - 'mapPartitionsWithIndex','mapPartitionsToDouble','mapPartitionsToPair'
		 */
		JavaRDD<Integer> rddData3 = rddData2.mapPartitions(new FlatMapFunction<Iterator<Integer>,Integer>() {
			
			public Iterator<Integer> call(Iterator<Integer> itr) {
				System.out.println(" lambda called in FlatMapFunction 2");
				List<Integer> list = new ArrayList<Integer>();
				while(itr.hasNext()) {
					Integer i = itr.next();
					System.out.println(i);
					list.add(i);	
				}	
				return list.iterator();	
			}
		},true);
		System.out.println(rddData3.collect());
		
		
		// just converting RDD into pair RDD(key value pair RDD).
		JavaPairRDD<Integer,Integer> rddData4 = rddData.mapToPair(new PairFunction<Integer,Integer,Integer>() {
			public Tuple2<Integer,Integer> call(Integer in) {
				return new Tuple2(in,1);
			}
		});
		rddData4.repartition(4);
		System.out.println(rddData4.collect());
			
		
		
		
		/*
		 * spark provides one more special variant of 'mapPartitions' called as 'mapPartitionsToPair', the function is executed for each partition and it returns key value pair RDD like 'mapToPair'.
		 * 
		 * NOTE: using 'mapPartions', we cannot return a key value pair. 
		 * NOTE: 'PairFlatMapFunction' should be used with 'mapPartitionsToPair'
		 * 
		 * Like how we write 'Integer' instead of 'Iterator<Integer>' in 'flatMap' methods. here also same rule applies for Tuple2 as well. instead of 'Iterator<Tuple2<Integer,Integer>>' we just 
		 * write 'Integer,Integer' in 'PairFlatMapFunction<Iterator<Integer>,Integer,Integer>()'
		 */
		JavaPairRDD<Integer,Integer> rddData5 = rddData3.mapPartitionsToPair(new PairFlatMapFunction<Iterator<Integer>,Integer,Integer>() {
			
			public Iterator<Tuple2<Integer,Integer>> call(Iterator<Integer> itr) {
				System.out.println(" lambda called in FlatMapFunction 3");
				List<Tuple2<Integer,Integer>> list = new ArrayList<Tuple2<Integer,Integer>>();
				while(itr.hasNext()) {
					Tuple2<Integer,Integer> i = new Tuple2<Integer,Integer>(itr.next(),1);
					System.out.println(i);
					list.add(i);	
				}	
				return list.iterator();	
			}
		});		
		System.out.println(rddData5.collect());
		
		
		
		
		
		/*
		 * We can also send a 'key value pair RDD' as input and get back a 'key value pair RDD' using 'mapPartitionsToPair' ash shown in the below code. 
		 */
		JavaPairRDD<Integer,Integer> rddData6 = rddData5.mapPartitionsToPair(new PairFlatMapFunction<Iterator<Tuple2<Integer,Integer>>,Integer,Integer>() {
			
			public Iterator<Tuple2<Integer,Integer>> call(Iterator<Tuple2<Integer,Integer>> itr) {
				System.out.println(" lambda called in FlatMapFunction 4");
				List<Tuple2<Integer,Integer>> list = new ArrayList<Tuple2<Integer,Integer>>();
				while(itr.hasNext()) {
					Tuple2<Integer,Integer> i = itr.next();
					System.out.println(i);
					/*
					 * INPORTANT NOTE!!!!!!!!!!!!!!! -->  To fetch the key and value of a tuple, we use '._1()' to ge the key and '._2()' to get the value.
					 */
					list.add(new Tuple2(i._1()+2,1));
				}	
				return list.iterator();	
			}
		});		
		
		System.out.println(rddData6.collect());
		
		
		
		/*
		 * Spark also provides one more RDD type called 'JavaDoubleRDD' and method called 'mapPartitionsToDouble'. 
		 * 
		 * 'JvaDoubleRDD' is nothing but 'JavaRDD<Double>'. 'mapPartitionsToDouble' method can be used only when we want transform RDD partitions to java RDD of Double type elements. we can achieve
		 * the purpose of 'mapPartitionsToDouble' using 'mapPartitions' as well as follows,
				  	'JavaRDD<Double> rddData7 = rddData.mapPartitions(new FlatMapFunction<Iterator<Integer>,Double>() {   ...... '
		   since using spark we usually transform values like Double, spark has given this specific method.
				 
			NOTE: 'DoubleFlatMapFunction' has to be used with 'mapPartitionsToDouble' method.		
			NOTE: no need to specify return type in 'DoubleFlatMapFunction'. i.e. instead of 'DoubleFlatMapFunction<Iterator<Integer>,Double>' we just give 'DoubleFlatMapFunction<Iterator<Integer>>'
			NOTE: instead of 'JavaRDD<Double> we have to give 'JavaDoubleRDD'
		 */
		JavaDoubleRDD rddData7 = rddData.mapPartitionsToDouble(new DoubleFlatMapFunction<Iterator<Integer>>() {
			
			public Iterator<Double> call(Iterator<Integer> itr) {
				List<Double> list = new ArrayList<Double>();
				while(itr.hasNext()) {
					list.add(itr.next().doubleValue());
				}
				return list.iterator();
			}
		});		
		System.out.println(rddData7.collect());
		
		
		
		
		/*
		 * Consider you want know the index number of the partion the lambda/Function is processing. How will you do that?
		 * Spark provides one more method called '.mapPartitionsWithIndex' in which the index of the partition that is being executed by the lambda/Function. It is sent as 'Integer' type.
		 * 
		 * As shown in the below code, the first argument sent by '.mapPartitionsWithIndex' will be index .i.e. 'i' in below code. the next argument sent is the iterator to the partition,  i.e. 'a'
		 * 
		 * NOTE: like '.mapPartitions', '.mapPartitionsToPair' and '.mapPartitionsToDouble' this also supports 'preservePartitions' argument but in this it is mandatory. because there is only one
		 * 		method provided i.e. 'mapPartitionsWithIndex(Function function,boolean preservePartitions)' where as for '.mapPartitions' and others has two methods like
		 * 		'mapPartitions(Function function)'  and 'mapPartitions(Function function,boolean preservePartitions)' hence even if we don't give the 'preservePartitions', it will take 
		 * 		'mapPartitions(Function function)' and doesn't give any error. But in '.mapPartionsWithIndex' if we call this without giving 'preservePartitions', it will give compile error since
		 * 		it doesn't have 'mapPartionsWithIndex(Function function)' method.
		 * 		 
		 */
		JavaRDD<Integer> rddData8 = rddData.mapPartitionsWithIndex( (i,a) -> {
			System.out.println("Index in lambda - " + i);
			return a;
		},true);
		System.out.println(rddData8.collect());
		
		
		
		
		/*
		 * The following code shows same as above code but with java function implementation instead of lambda.
		 * 
		 * NOTE: have to use 'Function2' only for function type.
		 * NOTE: The 'Function2' accepts 3 arguments here, first one is the index 'Integer' and the second is 'Iterator<Integer>' which is the iterator of input partition. and third is
		 * 'Iterator<Integer>' which is the iterator for output partition(we cannot give 'Integer' instead of 'Iterator<Integer>' like how we did in other methods for output type especially in this
		 * '.mapPartitionsWithIndex' method).
		 */
		JavaRDD<Integer> rddData9 = rddData.mapPartitionsWithIndex(new Function2<Integer,Iterator<Integer>,Iterator<Integer>>() {
			
			public Iterator<Integer> call(Integer index, Iterator<Integer> itr) {
				System.out.println("Index of the partition is - " + index);
				return itr;
			}
		},true);
		System.out.println(rddData9.collect());
		
		
		/*
		 * I couldn't find what '.mapPartitionsWithIndex$default$2' is all about. need to check. 
		 * ???????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????
		 */
		rddData.mapPartitionsWithIndex$default$2();
		
		
		
		
		
		
		
		
		
		
		/*
		 * Consider, i have an RDD without any partitions like as shown in the below code,
			    List<Integer> data = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14);	
				JavaRDD<Integer> rddData = sc.parallelize(data);
			Can i use '.mapPartitions' on this unpartitioned 'rddData'?
			Yes!!!! we can.
			In that case, the complete RDD is considered as one partition and the lambda/Function will be executed only once, in which the iterator for list of all elements will be passed to the
			lambda/Function.
		 * 
		 */
		
		
		
		
	}

}
