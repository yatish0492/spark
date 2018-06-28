package com.yatish.spark.transformations;

public class S1_What_is_Transformations {

	/*
	 * In spark, we might need to create new set of RDD from an existing RDD. this is called as transformation.
	 * eg: consider you have an RDD with number from 1 to 14. now for some operation you need RDD with numbers from 1 to 14, but all the numbers lesser than 5 should be -1. We use the first RDD and
	 * 		perform an operation to replace all the number lesser than 5 with -1 and assign it to a new RDD variable. this process of creating a new RDD from an existing RDD is called transformation.
	 * Demonstration of this example - refer to 'S2_map.java', 'rddData1' is being created using 'rddData'.
	 * 
	 * We got to know transformation, But how do we do it practically?
	 * Spark provides lot of functions like '.map()', '.flatmap()' methods to do this.
	 * 
	 */ 
}
