package com.yatish.spark.transformations;

public class S5_map_vs_flatMap {

	/*
	 * Map vs FlatMap
	 * ---------------
	 * Consider, you are transforming 'rddData1' to 'rddData2'
	 * Map
	 * ---
	 * The '.map' function returns a value. like if we call '.map' on set of 10 elements, then function/lambda will be executed for each element and return a value of same type.
	 * FlatMap
	 * -------
	 * The '.flatMap' function returns Iterator. like if we call '.flatMap' on set of 10 elements, then function/lambda will be executed for each element return a iterator of the collection of 
	 * elements of same type.
	 * 
	 * 
	 * Map
	 * ---
	 * '.map' is used if we want to map elements 'One to One'
	 * FlatMap
	 * -------
	 * '.flatMap' is used if we want to map elements 'One to Many'
	 * 
	 * 
	 * Map
	 * ---
	 * The number of elements in 'rddData1' will be same as number of elements in 'rddData2' after transformation. because, in '.map' it has to return a value in transformation logic hence each time
	 * transformation logic is called on an element in 'rddData1' there will be value returned to 'rddData2' hence the number of elements in 'rddData1' and 'rddData2' will always remain same but the
	 * values my get changed.
	 * eg: rddData1 --> ["acbd","defe","efefds"]				rddData2 --> ["xkd",null,null]		==> Values have been changed but number of elements remain same.
	 *     rddData1 --> [1,2,3,4,5,6,7]						rddData2 --> [1,0,-1,4,5,6,7]		==> Values have been changed but number of elements remain same. 
	 * FlatMap
	 * -------
	 * The number of elements in 'rddData2' can differ from 'rddData1' while transforming using '.flatMap'. since '.flatMap' returns iterator, that iterator can point to a collection of 'n' elements
	 * or to an empty collection.
	 *  eg: rddData1 --> ["acbd","defe","efefds"]				rddData2 --> ["xkd"]						==> Values have been changed and number of elements as well.
	 *     	rddData1 --> [1,2,3,4,5,6,7]						rddData2 --> [1,0,-1,4,5,6,7,8,9,10,11]		==> Values have been changed and number of elements as well.
	 *     
	 * Hence, using '.flatMap' we can achieve the transformation done by '.map', as we can return same number of elements as of the source RDD by returning iterator pointing to a single 
	 * element. But we cannot achieve a transformation of '.flatMap' using '.map' if the number of elements are changed in destination RDD during transformation.
	 * 
	 */	   
}
