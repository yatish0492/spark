package com.yatish.types.deployment;

public class S3_StandAlone_with_standby_Master_zookeeper {
	/*
	 * We had standalone deployment, then why we needed this?
	 * In stanalone deployment, if there is any worker/name node failure, we know that other nodes will take its load and process them. Hence high availability right but consider your master/data
	 * node fails, then!!!!! your whole cluster will be down. So only we have this mode of deployment, in which we configure standby master node, which will handle things if of master node fails.
	 * 
	 * So I can have only 1 standby master?
	 * No!!! you can have as many standby masters as you want.
	 * 
	 * I got it about why we need a standby master but why do we need zookeeper here?
	 * Utilizing ZooKeeper to provide leader election and some state storage, you can launch multiple Masters in your cluster connected to the same ZooKeeper instance. One will be elected “leader” 
	 * and the others will remain in standby mode. If the current leader dies, another Master will be elected, recover the old Master’s state, and then resume scheduling. The entire recovery process 
	 * (from the time the first leader goes down) should take between 1 and 2 minutes. Note that this delay only affects scheduling new applications – applications that were already running during 
	 * Master fail over are unaffected.
	 * 
	 */
}
