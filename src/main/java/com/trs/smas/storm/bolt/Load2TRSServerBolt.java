package com.trs.smas.storm.bolt;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

import com.trs.client.TRSConnection;
import com.trs.client.TRSException;
import com.trs.smas.storm.util.CSVUtil;

public class Load2TRSServerBolt extends BaseBasicBolt {

	private static final long serialVersionUID = -7489261520275127899L;
	
	private String [] fieldNames;
	
	private String host;
	private String port;
	private String username;
	private String password;
	private String database;
	TRSConnection connection;
	
	public Load2TRSServerBolt(String host,String port,String username,String password,String database,String fieldNames) throws TRSException{
		this.host = host;
		this.port = port;
		this.username = username;
		this.password = password;
		this.database = database;
		this.fieldNames = CSVUtil.parse(fieldNames);
	}
	
	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		String [] fieldValues = (String []) input.getValue(1);
		
		StringBuilder sb = new StringBuilder();
		for(int i = 0 ; i < fieldNames.length ; i ++ ){
			sb.append(fieldNames[i]).append("=").append(fieldValues[i]).append("\n");
		}
		sb.deleteCharAt(sb.length()-1);

		try {		
			if(connection == null){
				connection = new TRSConnection();
				connection.connect(host, port, username, password);
				connection.setMaintOptions('\n', "", "", 0, 0);
			}
			connection.executeInsert(database, username, sb.toString());
		} catch (TRSException e) {
			collector.reportError(e);
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

	@Override
	public void cleanup() {
		if(connection != null){
			connection.close();
			connection = null;
		}
		super.cleanup();
	}

	
	abstract public class DatabaseSelector{
		public abstract String select(Tuple input);
	}
}
