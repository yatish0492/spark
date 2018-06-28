package com.yatish.spark.persistance;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;

public class S2_Why_Persistance {

	public static void main(String[] args) {
	
		/*
		 * Why do we need Persistence in spark?
		 * Spark doesn't keep any RDDs once the action on them is called. like in this program, when 'rddData1.count()' is called, the transformation will happen and 'rddData1' will be calculated and
		 * 'rddData1.count()'  will be printed. Once spark prints 'rddData1.count()', it will delete the RDDs used for this action, like it will delete 'rddData1' and 'rddData'. Hence next time when
		 * 	execute 'rddData1.count()', spark will again recompute 'rddData1' since it has not stored 'rddData1' when it was previously calculated. In this case it is just one transformation, it will
		 * take less than a second so it's fine but in production there will be lot of transformations and lot of data hence it will take more time. So in production for each and everytime action is
		 * called, we cannot keep recomputing/re-transform from the scratch.
		 * Solution
		 * --------
		 * Persistence will solve this problem. we can persist(save/cache) a RDD/dataFrame/dataSet so that next time we call an action on 'rddData1', it will not be recalculated from scratch, the 
		 * value will be fetched from memory.
		 * 
		 * one more issue is consider there are lot of actions on 'rddData2', then during each and every action the 'rddData2' will be computed from 'rddData'. so if we persist 'rddData2' then the
		 * transformation from 'rddData2' from 'rddData' will happen only once and the persisted 'rddData2' will be fetched from memory each and every-time action is called hence no need to do the 
		 * transfromation from 'rddData' to 'rddData2' each and everytime action is called.
		 * 
		 */
		SparkConf conf = new SparkConf().setAppName("SparkStart").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		List<Integer> data = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14);	
		JavaRDD<Integer> rddData = sc.parallelize(data);
		
		JavaRDD<Integer> rddData1 =	rddData.map(a -> {
			System.out.println("Executing the map funtion to create rddData1");
			return a;
		});
		
		JavaRDD<Integer> rddData2 =	rddData1.map(a -> {
			System.out.println("Executing the map funtion to create rddData1");
			return a;
		});
		
		// We are telling spark to store the value 'rddData1' so that next time any action performed on it will fetch the value from memory instead again doing the transformations from scratch.
		rddData1.persist(StorageLevel.MEMORY_ONLY());
		System.out.println(rddData1.count());
		
		//When this statement is executed, it will not do the transformations again as we have called '.persist' method on 'rddData1'. If we comment '.persist' call method in this program then
		//the transformations of 'rddData' to 'rddData1' will be executed again.
		System.out.println(rddData1.count());
		
		
		/*Here we have not called '.persist' on 'rddData2' so 'rddData2.count()' will compute from begining?
		 * No!!! it will fetch the 'rddData1' from memory and execute only the 'rddData1' to 'rddData2' transformation.
		 * 
		 */
		System.out.println(rddData2.count());
		
		
		//rddData.checkpoint();

	}

}
