package com.trs.smas.storm.bolt;

import java.io.File;

import org.apache.log4j.Logger;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

import com.trs.client.RecordReport;
import com.trs.client.TRSConnection;
import com.trs.client.TRSConstant;
import com.trs.client.TRSException;

public class Load2TRSServerBolt extends BaseBasicBolt {

	private static final Logger LOG = Logger.getLogger(Load2TRSServerBolt.class);
	
	private static final long serialVersionUID = -7489261520275127899L;
	
	private String host;
	private String port;
	private String username;
	private String password;
	TRSConnection connection;
	
	public Load2TRSServerBolt(String host,String port,String username,String password) throws TRSException{
		this.host = host;
		this.port = port;
		this.username = username;
		this.password = password;
	}
	
	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		String database = input.getString(0);
		String fileName = input.getString(1);
		LOG.info("loading " + fileName + " to "+username+"@"+host+":"+port+"/"+database);

		try {		
			if(connection == null){
				connection = new TRSConnection();
				connection.connect(host, port, username, password);
				TRSConnection.setCharset(TRSConstant.TCE_CHARSET_UTF8, false);
			}
			RecordReport r = connection.loadRecords(database, username, fileName, null, false);
			LOG.info(fileName + " loaded, success : "+r.lSuccessNum + ",failure : "+r.lFailureNum);
			new File(fileName).delete();
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

}
