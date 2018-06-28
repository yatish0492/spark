package com.yatish.DataFrame;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.yatish.Pojo.Person;

public class S2_DataFrame_From_JSON_File_And_Querying_And_Views {

	public static void main(String[] args) {
		
		
		SparkSession session1 = SparkSession.builder().master("local").appName("sparkSQL").getOrCreate();
		SparkSession session2 = SparkSession.builder().master("local").appName("sparkSQL").getOrCreate();
		/*
		 * !!IMPORTANT!!
		 * -------------
		 * The JSON file should have the json objects one in a line, otherwise the spark parser will give consider it as corrupt data.
		 * eg: Valid -->  	{"name":"yatish1","age":"27"},
							{"name":"yatish1","age":"27"} 
			   Invalid --> 	{									//This is invalid because, each object should come in one line. literally each row data in table should be present in each line in 
			   					"name":"yatish1",				//json file
			   					"age":"27"
			   				},
		 */
		Dataset<Row> df = session1.read().json("src/main/resources/data.json");
		
		//This will show the complete data in 'df' in tabular form.
		df.show();
		
		//This will print the schema of 'df' content in tabular representation.
		df.printSchema();
		
		//If you want to use 'select' on the query then you can call '.select' on it.
		df.select("name").show();
		
		//If you want to specify more than one column in the 'select' then you have to provide them using 'df.col' comma seperated. we can perform few more operations on the column by calling
		//functions like '.plus' on 'df.column'. here '.plus(1)' will add each element of the column with 1.
		df.select(df.col("name"),df.col("age").plus(1)).show();
		
		//we can call '.gt' on the 'df.col' to specify conditions on the column as shwon below.
		df.select(df.col("age").gt(25)).show();
		
		// we can use '.filter' as well.
		df.filter(df.col("age").gt(25)).show();
		
		// we can use 'groupBy' as well.
		df.groupBy(df.col("name")).count().show();
		
		// we can use 'orderBy' as well
		df.orderBy(df.col("name").desc()).show();
		
		//If we are using this df repetatively, then better to persist in memory. there is one more alternative to persit() that is cache().
		df.cache();
		
		// we can create views as well like how we create views in SQL.
		df.createOrReplaceTempView("LocaltempView");
		
		/*
		 * What are views?
		 * Views are known as virtual table. But internally SQL will save only the query in the DB, It will not even store the view contents. Main advantage of view is as follows,
		 * 1) Security. we can grant permission to a community at the table level and view level as well. consider you grant access for an employee to employees table which contains table as well.
		 * 		So, an employee can view complete table contents which contains the salary of other employees as well which is confidential. hence we can create a view with query say 'SELECT * FROM
		 *  		WHERE employee_name="name"' so that he will be able to see only the details of him not other employees and grant him the access to the view. This is how view helps.
		 * 
		 * 
		 * Why The hell do we need views here?
		 * On thing why we need views is the same obvious reason, that why do SQL need views. Not only that we can write queries like in SQL to query as shown in below code,
		 */
		session1.sql("SELECT * FROM LocaltempView").show();
		
		//If you want to store the query result in a variable then don't call '.show()' and assign it to variable as shown below
		Dataset<Row> queryResult = session1.sql("SELECT * FROM LocaltempView");
		
		/*
		 * We create view of a single table? if that is the case what is the point in creating a view, we can directly use table/dataFrame only right?
		 * No!!! we can create view using queries like select/join etc. Refer to the below code for real time example.
		 */
		df.select(df.col("age").gt(25)).createOrReplaceTempView("peopleWithAgeGreaterThan25");
		
		
		/*
		 * There is one more way of creating Views using '.createGlobalTempView'.
		 * 
		 * What is the difference between creating view using '.createOrReplaceTempView' and '.createGlobalTempView'?
		 * '.createOrReplaceTempView'
		 * --------------------------
		 * using this, the view will be created locally. that means, The view will be available until the session is open and will be destroyed once the session is closed. consider the 
		 * 'peopleWithAgeGreaterThan25' view created in above code on the dataFrame 'df' which was created from 'session1'.Once we close the 'session1' using 'session1.stop()', the view will be
		 * deleted. we cannot execute or access this view from any other session like 'session2'. Even if we try to access it as shown below, it will give compilation error.
		 * '.createGlobalTempView'
		 * -----------------------
		 * using this, the view will be created globally, that means, The view will available even after the session is closed and it will be deleted only when the spark application terminates
		 * irrespective of sessions closing. In this case, in the below code even after 'session1.stop()', 'peopleWithAgeGreaterThan25' view will be accessible and 'session2' will give the result
		 * without any compilation error.
		 * 
		 * 
		 * So in '.createOrReplaceTempView' the view of a session cannot be accessed by other session is what you mean?
		 * No!!! irrespective of whether the view is created from '.createOrReplaceTempView' or '.createGlobalTempView', we can access the view from any session like as follows,
		 * 		session1.sql("SELECT * FROM peopleWithAgeGreaterThan25").show();
		 * 		session2.sql("SELECT * FROM peopleWithAgeGreaterThan25").show();
		 * only thing is if view is created using '.createOrReplaceTempView' from 'session1', that view can be accessed from any session like 'session1' or 'session2' until 'session1' is closed.
		 * whereas if the view is created using '.createGlobalTempView' from 'session1', the view can be accessed from any session like 'session1' or 'session2' even after 'session1' is closed.
		 * 
		 */
		//df.createGlobalTempView("globalTempView");  //This is not supported in current version of spark but it is there in latest version
		session1.stop();
		session2.sql("SELECT * FROM peopleWithAgeGreaterThan25").show();
		
		
		
		/*
		 * DataFrame also performs all operations supported on RDD like 'map' etc.
		 */
		Encoder<Person> personEncoder = Encoders.bean(Person.class);
		Dataset<Row> mapDoneParentDataSet = df.map(a -> {
			return a;
			},personEncoder);
		mapDoneParentDataSet.show();		

	}

}
