package com.trs.smas.storm.util;

public class StringUtil {

	public static String avoidNull(String str){
		return str==null?"":str;
	}
	
	public static String join(String [] ary){
		StringBuilder sb = new StringBuilder();
		for(String item : ary){
			sb.append(item).append(",");
		}
		return sb.deleteCharAt(sb.length()-1).toString();
	}
}
