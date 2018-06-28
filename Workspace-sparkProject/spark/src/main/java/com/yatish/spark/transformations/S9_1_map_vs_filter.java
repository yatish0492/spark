package com.yatish.spark.transformations;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;

public class S9_1_map_vs_filter {
	
	/*
	 * map
	 * ---
	 * This will return equal number of elements back. like if we do '.map' transformation an RDD A to B, If A has 10 elements, then B also will have 10 elements.
	 * filter
	 * ------
	 * This will return a subset/equal number of elements back. like if we do '.filter' transformation an RDD A to B, If A has 10 elements, then B will have either less than 10 or equal to 10(if all
	 * elements pass filter condition).
	 * 
	 * 
	 * map
	 * ---
	 * The map function/lambda can return any type. like it can accept string and return an integer etc.
	 * 		eg: 
	 * 			List<Integer> data = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14);
	 * 			JavaRDD<Integer> distData = sc.parallelize(data);
	 * 			JavaRDD<String> distData = distData.map(a -> {				// we are accepting JavaRDD<Integer> but returning JavaRDD<String>. i.e. Integer to String
	 * 							if(a%2 == 0) {
	 * 									return "even";
	 * 							} else {
	 * 									return "odd";
	 *							});
	 *
	 * filter
	 * ------
	 * This filter function/lambda cannot return any type. it has to return which ever the type that is being sent as input. like we cannot perform '.filter' on JavaRDD<Integer> and return
	 * JavaRDD<String>, we have to return JavaRDD<Integer>.
	 * 
	 * 
	 * map
	 * ---
	 * map function/lambda can return any type like 'String','Integer' etc.
	 * filter
	 * ------
	 * filter function/lambda can only return 'boolean' type since it is a predicate function.
	 */
}
