package com.trs.smas.storm.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import com.google.common.base.Preconditions;

public class PropertiesUtil {

	public static Properties loadProperties(String fileName){
		return loadProperties(new File(fileName));
	}
	
	public static Properties loadPropertiesFromClasspath(String fileName){
		Preconditions.checkNotNull(fileName);
		InputStream is = PropertiesUtil.class.getClassLoader().getResourceAsStream(fileName);
		Preconditions.checkNotNull(is);
		Properties props = new Properties();
		
		try{
			props.load(is);
		}catch(Exception e){
		}finally{
			try {
				if(is != null) is.close();
			} catch (IOException e) {
			}
		}
		return props;
	}
	
	public static Properties loadProperties(File f){
		Preconditions.checkNotNull(f);
		Preconditions.checkArgument(f.isFile() && f.canRead());
		Properties props = new Properties();
		
		InputStream is = null;
		try {
			is = new FileInputStream(f);
			props.load(is);
		} catch (Exception e) {
		} finally {
			try {
				if(is != null) is.close();
			} catch (IOException e) {
			}
		}
		
		return props;
	}
	
}
