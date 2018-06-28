package com.yatish.types.deployment;

public class S2_Standalone {

	/*
	 * What is standalone mode of deployment?
	 * Spark is self-independent. It has its own resource manager etc. If we just use the spark downloaded file for deployment without any integration with other things like yarn/mesos etc. 
	 * Then that mode is called standalone.
	 * 
	 * How to setup the spark standalone cluster?
	 * Master commands
	 * ---------------
	 * sbin/start-master.sh	--> This will start the spark master on the current machine. once you start the master, you can view the spark UI in the current machine at 'localhost:8080'
	 * Slave command
	 * -------------
	 * './sbin/start-slave.sh spark://10.232.12.82:7077' --> This will start/register the slave node in the specified master(10.232.12.82) node.
	 * sbin/stop-slaves.sh	--> This will stop all the slaves instances running on the current machine.
	 * On Master/Slave
	 * ---------------
	 * bin/spark-submit --class "com.yatish.spark.transformations.S3_collect" --master spark://10.232.12.82:7077 ../Workspace-sparkProject/spark/target/spark-1.0.jar  --> This command will submit the
	 * 									jar file and executes the class file passed with '--class' command line argument.
	 * 							NOTE: we can specify the 'master' node to which it should be submitted using '--master' command line argument but, if the executing class 'S3_collect' has 'SparkConf'
	 * 								   then the 'master' specified in the 'SparkConf' will override the '--master' command line argument sent. Consider, you send 'spark://10.232.12.82:7077' in the 
	 * 									'--master' command line argument and in 'S3_collect', you have set master as 'local' in 'SparkConf', in this case 'local' will be considered instead of 
	 * 									'spark://10.232.12.82:7077'. '--master' command line argument is optional. 
	 */

}
