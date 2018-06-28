package com.test;

public class Polaries_version_Logic {

	public static void main(String[] args) {
		
		String version="16.03.01a";
		
		String[] subVersions = version.split("\\.");
        int majorVersion = -1;
        int subMinorVersion = -1;
        int minorVersion = -1;
        for (int i = 0; i < subVersions.length; i++) {
            if (subVersions[i].contains("(")) {
                subVersions[i] = subVersions[i].substring(0, subVersions[i].indexOf("("));
            }
            subVersions[i] = subVersions[i].replaceAll("[^0-9|.]", "");
            
            if (i == 0) {
                majorVersion = Integer.parseInt(subVersions[i].trim());
            } else if (i == 1) {
                subMinorVersion = Integer.parseInt(subVersions[i].trim());
            } else if (i == 2) {
                minorVersion = Integer.parseInt(subVersions[i].trim());
            }
        }
        System.out.println("Done");
	}
	

}
