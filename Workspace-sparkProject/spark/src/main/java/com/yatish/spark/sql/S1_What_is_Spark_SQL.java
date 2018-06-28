package com.yatish.spark.sql;

public class S1_What_is_Spark_SQL {

	/*
	 * What is Spark SQL?
	 * It is a component provided by spark which will help us to execute SQL queries on the data/DataFrame/Dataset. It also provides support to read/write data from an existing HIVE installation.
	 * 
	 * 
	 * What is so special about Spark SQL, we can use Hibernate also right, which will enable us to use HQL instead of SQL?
	 * Don't think that it just allows us to execute SQL queries. Spark-SQL, provides very good efficiency compared to others like Hibernate etc.
	 * 
	 * 
	 * What optimization does Spark SQL do?
	 * In depth of Spark SQL, there lies a 'Catalyst Optimizer'. Catalyst optimizer supports both 'rule-based' and 'cost-based' optimization.
	 * Rule-based Optimization
	 * -----------------------
	 * It uses set of rules to determine how to execute the query.
	 * Cost-based Optimization
	 * -----------------------
	 * Multiple plans are generated based on the rules. at last the cost of each plan is calculated and the plan with lowest cost is considered and executed.
	 * 
	 * NOTE:
	 * -----
	 * Spark-SQL deals with both SQL queries and DataFrame/DataSet API.
	 * 
	 * 
	 * 
	 * What is the phases, Spark SQL do in its operations?
	 * 1) Analyze
	 * 		It will analyze the operation/queries
	 * 2) Logical Optimization
	 * 		It will optimize the logic based on 'Rule-based' and 'cost-based'
	 * 3) Physical planning
	 * 4) Code Generation
	 * 		It will generate the code, to perform the operation. Like if we give 'SELCT * FROM ...', then it is not an SQL DB to execute it directly, Spark SQL will convert into corresponding code
	 * 		which can do this operation. 
	 * 
	 * 
	 * 
	 * 
	 * 
	 */

}
