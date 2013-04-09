package com.trs.smas.storm.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class DateUtil {

	public static final String DEFAULT_INPUT_FORMAT = "yyyy-MM-dd";
	public static final String TRSSERVER_DATE_FORMAT = "yyyy.MM.dd hh:mm:ss";
	
	private static final SimpleDateFormat INPUT_FORMAT = new SimpleDateFormat(DEFAULT_INPUT_FORMAT);
	private static final SimpleDateFormat OUTPUT_FORMAT = new SimpleDateFormat("yyyyMMdd");
	private static final SimpleDateFormat TRSSERVER_FORMAT = new SimpleDateFormat(TRSSERVER_DATE_FORMAT);
	
	
	/**
	 * 格式 yyyyMMdd
	 * @param date
	 * @return
	 */
	public static String nextMonthFirstDay(String date){
		Calendar lastDate = Calendar.getInstance();
		try {
			lastDate.setTime(INPUT_FORMAT.parse(date));
		} catch (ParseException e) {
			return null;
		}
		lastDate.set(Calendar.DATE, 1); // 设为当前月的1号
		lastDate.add(Calendar.MONTH, 1); // 一个月，变为下月的1号
		return OUTPUT_FORMAT.format(lastDate.getTime());
	}
	
	public static String formatForTRSServer(long time){
		return TRSSERVER_FORMAT.format(new Date(time));
	}
	
}
