package com.yatish.spark.transformations;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class S3_1_take {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("SparkStart").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		List<Integer> data = Arrays.asList(10, 11, 12, 13, 14, 1, 2, 3, 4, 5, 6, 7, 8, 9);	
		JavaRDD<Integer> rddData = sc.parallelize(data);
		
		/*
		 * It is same like 'collect()' but it will not bring all the rdd elements to the master node. It will just bring how my elements we specify as parameter.
		 * 
		 * In 'collect()' there was problem, there was a chance of getting 'out of memory' exception due to bringing all the rdd elements to master node. To avoid this problem, 'take' has been 
		 * introduced to bring only a subset of the rdd data. 
		 * 
		 * Eg: As shown in the below code, we are just bringing only 4 elements from the RDD to master node.
		 *
		 */
		System.out.println(rddData.take(4));
		
		
		/*
		 * 'take' method will not return the array in sorted way. it will be in unsorted way. To get the sorted way, spark provides one more method 'takeOrdered(int)'. By default it will sort in 
		 * ascending order.
		 * 
		 * Eg: As shown in the below code.
		 */
		System.out.println(rddData.takeOrdered(4));
		
		
		/*
		 * 'takeOrdered(int)' method will return the sorted order in ascending order. Spark provides, one more variation of 'takeOrdered' which accepts, type of sorting, in which you can pass the
		 * comparator as argument according to which it will sort.
		 * 
		 * Eg: as shown in the below code.
		 *
		 */
		System.out.println(rddData.takeOrdered(4, Collections.reverseOrder()));
		
		
		/*
		 * Consider, you want to take sample of the rdd data instead of getting rdd elements in a sequence. then spark provides one more method 'takeSample(boolean samplingType,int size)'. This will
		 * provide us sampled data.
		 * 
		 * What is sampled data?
		 * Let us consider normal real life use-case, consider a government officer is taking sample of road to test its quality, he will not just dig and take the materials at the end of the road. 
		 * he will take it randomly in some places wherever he wants like in middle,end,side of the road etc. 
		 * In this case consider we have data '10, 11, 12, 13, 14, 1, 2, 3, 4, 5, 6, 7, 8, 9', in this if we want to take sample of this data, consider i will take 4 elements as sample. if we take 
		 * elements randomly across the elements instead of taking 4 elements present in beginning or end.
		 * 
		 * How can we get sample of the rdd data?
		 * We know that 'takeOrdered','take' will give the elements in some order but not randomly picked hence it doesn't give us sampled data. So only spark provides one more method 'takeSample'
		 * method.
		 * There are 2 variations of 'takeSample' method as show below,
		 * 		takeSample(boolean withReplacement,int numOfElements)
		 * 		takeSample(boolean withReplacement,int numOfElements,int seed);
		 * withReplacement --> in sampling, there are 2 types 
		 * 					   1) with Replacement --> Consider, you have put chits in a bowl and you want 2 samples of it. now i randomly prick up one chit and then i will put that chit back 
		 * 											   into the bowl and then i will pick the second one. In this case there is a chance that, i can get the same chit which i put it back in 
		 * 												second pick. Hence there is a possibility that we can get the same element/chit during sampling.
		 * 					   2) witout Replacement --> Consider, you have put chits in a bowl and you want 2 samples of it. now i randomly prick up one chit and then i will not put that chit back
		 * 												into the bowl and then i will pick the seccond one. In this case there is a chance that, i cannot get the same chit which i had i had picked
		 * 												first because, i have not put that chit back to bowl. Hence there is no possibility that we can get the sample element/chit during sampling.
		 * seed  -->  in sampling, there is a random rdd element picker algorithm. the random number algorithms take an argument during initialization, that initialized number is called as seed. 
		 * 
		 * Example: as shown in the below code.
		 * 
		 * 
		 */
		System.out.println(rddData.takeSample(true, 4));  // example of takeSample(boolean withReplacement,int numOfElements)
		System.out.println(rddData.takeSample(false, 4)); // example of takeSample(boolean withReplacement,int numOfElements)
		System.out.println(rddData.takeSample(true, 4, 5));  // example of takeSample(boolean withReplacement,int numOfElements,int seed)

	}

}
