package com.yatish.spark.transformations;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class S13_Sample {

	public static void main(String[] args) {
		
		SparkConf conf = new SparkConf().setAppName("SparkStart").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		List<Integer> data = Arrays.asList(10, 11, 12, 13, 14, 1, 2, 3, 4, 5, 6, 7, 8, 9);	
		JavaRDD<Integer> rddData = sc.parallelize(data);
		
		/*
		 * It is same as what we discussed in 'S3_1_take.java'  regarding '.takeSample'. But Only difference is that '.takeSample' will return us a Collection/List whereas '.sample' will return us
		 * RDD of sample.
		 * 
		 * One more difference is that in '.takeSample' we will mention the second arguments as 'int num' which was number elements to be returned in the sample, But in '.sample', we will mention
		 * the second argument as 'double fraction' which will specify the fraction of the elements to be returned like if we specify '0.1', that means, returned sampled elements should be 10% of
		 * the size of source RDD.
		 */
				
		JavaRDD<Integer> rddData1 = rddData.sample(true, 0.1); // example of sample(boolean withReplacement,double fraction)
		JavaRDD<Integer> rddData2 = rddData.sample(false, 0.1); // example of sample(boolean withReplacement,double fraction)
		JavaRDD<Integer> rddData3 = rddData.sample(false, 0.1,4); // example of sample(boolean withReplacement,double fraction, int seed)

	}

}
