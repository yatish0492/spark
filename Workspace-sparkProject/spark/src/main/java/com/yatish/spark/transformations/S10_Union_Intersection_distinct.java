package com.yatish.spark.transformations;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class S10_Union_Intersection_distinct {

	public static void main(String[] args) {
		
		
		SparkConf conf = new SparkConf().setAppName("SparkStart").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		List<Integer> data = Arrays.asList(1, 2, 2, 3, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15);
		List<Integer> data1 = Arrays.asList(8, 9, 10, 11, 12, 13, 14, 15, 16);
		List<Integer> data2 = Arrays.asList(15, 16, 17, 18, 19, 20);
		JavaRDD<Integer> rddData = sc.parallelize(data);
		JavaRDD<Integer> rddData1 = sc.parallelize(data1);
		JavaRDD<Integer> rddData2 = sc.parallelize(data2);
		
		
		/*
		 * Union
		 * -----
		 * It is same as union of sets. It will take the union of elements of all the RDDs and return it.
		 * In this case, the union of 'rddData','rddData1' and 'rddData2' is taken and assigned to 'rddData3'
		 * 
		 * So we can do union of only 3 RDDs?
		 * No!!!
		 * It is unlimited. you can do union of 2 or 3 or 4 or more.
		 * 
		 * 'rddDate3' value --> [1, 2, 2, 3, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 8, 9, 10, 11, 12, 13, 14, 15, 16, 15, 16, 17, 18, 19, 20]
		 */
		JavaRDD<Integer> rddData3 = rddData.union(rddData1).union(rddData2);
		
		
		
		/*
		 * Intersection
		 * -----
		 * It is same as Intersection of sets. It will take the Intersection of elements of all the RDDs and return it.
		 * In this case, the Intersection of 'rddData','rddData1' and 'rddData2' is taken and assigned to 'rddData4'
		 * 
		 * So we can do Intersection of only 3 RDDs?
		 * No!!!
		 * It is unlimited. you can do Intersection of 2 or 3 or 4 or more.
		 * 
		 * 'rddDate3' value --> [15]
		 */
		JavaRDD<Integer> rddData4 = rddData.intersection(rddData1).intersection(rddData2);
		
		
		
		/*
		 * Distinct
		 * -----
		 * It will find the distinct elements in the RDDs. It will take the distinct of elements of all the RDDs and return it.
		 * In this case, the distinct elements of 'rddData' is taken and assigned to 'rddData5'
		 * 
		 * When is Distinct transformation Helpful?
		 * It is helpful to remove duplicate data.
		 * 
		 * So we can do distinct on only 3 or more RDDs?
		 * No!!!
		 * It can be applied only on one RDD.
		 * 
		 * 'rddDate3' value --> [13, 15, 4, 11, 14, 1, 6, 3, 7, 9, 8, 12, 10, 5, 2]
		 */
		JavaRDD<Integer> rddData5 = rddData.distinct();
		
		
		
		/*
		 * spark provides one more method for distinct. i.e. '.distinct(int numOfPartitions)'. based on the number of partitions we mention as argument, spark will create distinct set of elements
		 * of RDD and then partition it and give it.
		 * 
		 * In Below code, 'rddData' RDD's distinct elements are partitioned into 5 partitions/slices and assigned to 'rddData6'
		 * 
		 */
		JavaRDD<Integer> rddData6 = rddData.distinct(5);
		
		
		
		
		JavaPairRDD<Integer,Integer> rddData7 = rddData.cartesian(rddData1);  // This is nothing but a cross join. This will return 'JavaPairRDD' note 'JavaRDD'
		JavaRDD<Integer> rddData8 = rddData.subtract(rddData1);  // This is set operation 'A-B'

	}

}
