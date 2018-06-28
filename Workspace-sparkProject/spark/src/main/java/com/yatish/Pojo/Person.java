package com.yatish.Pojo;

import java.io.Serializable;

/*
 * !!!!!!!!!!! IMPORTANT !!!!!!!!!!!!!!!!!!
 * Make sure to implement Serializable on all pojo classes used in spark, because they have to be transferred across the data/worker nodes.
 */
public class Person implements Serializable{
	
	String name;
	long age;

	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public long getAge() {
		return age;
	}
	public void setAge(long age) {
		this.age = age;
	}
	
}