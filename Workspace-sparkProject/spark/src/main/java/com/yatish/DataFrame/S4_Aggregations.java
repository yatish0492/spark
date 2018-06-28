package com.yatish.DataFrame;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class S4_Aggregations {

	/*
	 * What is Aggregations?
	 * Aggregations are nothing but the 'sum','min','max','avg' etc functions we have in SQL.
	 */
	public static void main(String[] args) {
		
		SparkSession session1 = SparkSession.builder().master("local").appName("sparkSQL").getOrCreate();
		Dataset<Row> df = session1.read().json("src/main/resources/data.json");
		
		df.createOrReplaceTempView("df_view");
		
		/*
		 * Actually spark provides default aggregation functions like count(), countDistinct(), avg(), max(), min(), etc.
		 * 
		 * So i can use the aggregation function like 'df.select("age").max()'?
		 * No!!! Unfortunately spark doesn't support it as of now because user can also write it as 'df.select("age").max()', which will give runtime error since max() cannot be applied on strings.
		 * 
		 * So how can we apply aggregations as it is very important thing?
		 * Don't worry there is always a solution. we can use aggregation functions on views in traditional way of query as shown in below code.
		 */
		session1.sql("SELECT avg(age) as Average_Age FROM df_view").show();
		
		
		
		/*
		 * We can create custom user-defined aggregate functions as well. refer to - https://spark.apache.org/docs/latest/sql-programming-guide.html#aggregations
		 */

	}

}
