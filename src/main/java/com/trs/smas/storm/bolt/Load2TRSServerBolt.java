package com.trs.smas.storm.bolt;

import java.io.File;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

import com.trs.client.RecordReport;
import com.trs.client.TRSConnection;
import com.trs.client.TRSConstant;
import com.trs.client.TRSException;
import com.trs.smas.storm.util.CSVUtil;

public class Load2TRSServerBolt extends BaseBasicBolt {

	private static final long serialVersionUID = -7489261520275127899L;
	
	private String host;
	private String port;
	private String username;
	private String password;
	private String database;
	TRSConnection connection;
	
	public Load2TRSServerBolt(String host,String port,String username,String password,String database) throws TRSException{
		this.host = host;
		this.port = port;
		this.username = username;
		this.password = password;
		this.database = database;
	}
	
	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		String database = input.getString(0);
		String fileName = input.getString(1);
		System.out.println("==================================================\n"
				+"load " + fileName + " to "+database
				+"\n==================================================");
//		String [] fieldValues = (String []) input.getValue(1);
//		
//		StringBuilder sb = new StringBuilder();
//		for(int i = 0 ; i < fieldNames.length ; i ++ ){
//			sb.append(fieldNames[i]).append("=").append(fieldValues[i]).append("\n");
//		}
//		sb.deleteCharAt(sb.length()-1);
//
		try {		
			if(connection == null){
				connection = new TRSConnection();
				connection.connect(host, port, username, password);
				TRSConnection.setCharset(TRSConstant.TCE_CHARSET_UTF8, false);
			}
			RecordReport r = connection.loadRecords(database, username, fileName, null, false);
			System.out.println(r.lFailureNum);
			if(r.lFailureNum == 0){
				new File(fileName).delete();
			}
		} catch (TRSException e) {
			collector.reportError(e);
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

	@Override
	public void cleanup() {
//		if(connection != null){
//			connection.close();
//			connection = null;
//		}
		super.cleanup();
	}

	
	abstract public class DatabaseSelector{
		public abstract String select(Tuple input);
	}
}
