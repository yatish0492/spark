package com.yatish.datasets;

public class DataFrame_vs_DataSet {

	/*
	 * 
	 * DataFrame
	 * ---------
	 * It is a subset of DataSet, A DataSet of type as 'Row' is called as DataFrame.
	 * DataSet
	 * -------
	 * DataSet can be of any type like 'Dataset<String>', 'Dataset<Person>' etc.
	 * 
	 * 
	 * DataFrame
	 * ---------
	 * We can convert 'JavaRDD<Person>' to a dataFrame 'Dataset<Row>' using 'createDataFrame' function but we cannot convert in reverse order, a dataFrame 'Dataset<Row>' into Person RDD 
	 * 'JavaRDD<Person>', we can only convert the dataFrame 'Dataset<Row>' into row RDD 'JavaRDD<Row>'. because there is no direct method supported on the dataFrame to convert it to JavaRDD 
	 * of other type like String,Person etc. It will return only of type 'Row'
	 * DataSet
	 * -------
	 * Using DataSet we can overcome the limitation mentioned above for DataFrame. Using DataSet, we can convert 'JavaRDD<Person>' to a dataFrame 'Dataset<Person>' and do reverse order as well.
	 * 
	 * DataFrame
	 * ---------
	 * It is not Type Safe.
	 * We know that we can query the dataFrame like 
	 * 		'df.filter(df.col("age").gt(21));'  --> here since we giving property as string "age", at coding/compile time, it won't give any error if we give 'abcd' instead of "age" only at run-time 
	 * 												it will fail saying that 'abcd' column is not present in the 'Row'
	 * DataSet
	 * -------
	 * It is Type Safe.
	 * We can query the DataSet like dataFrame as shown above and we have one more way as well apart from it as shown below,
	 * 		'dataset.filter(person -> person.getAge() < 21);  -->  Here since we are calling 'getAge()' function to fetch 'age' user cannot do anything wrong here like providing 'getAbcd' etc as we
	 * 															  will get a compile time wanting that 'getAbcd' is not defined for 'Person' class. Hence we can catch these kind of problem during
	 * 															  compile time and fix them instead of application failing at runtime.
	 */
}
