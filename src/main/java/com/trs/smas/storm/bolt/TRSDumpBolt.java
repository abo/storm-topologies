package com.trs.smas.storm.bolt;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Serializable;
import java.io.Writer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

import backtype.storm.Config;
import backtype.storm.Constants;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.trs.smas.storm.util.DateUtil;
import com.trs.smas.storm.util.StringUtil;

public class TRSDumpBolt extends BaseBasicBolt {
	private static final Logger LOG = Logger.getLogger(TRSDumpBolt.class);

	private static final long serialVersionUID = -4481643466309672632L;
	
	private String [] fieldNames;
	private DatabaseSelector dbSelector;
	private long emitSize;
	private long timeout;
	
	Map<String,TRSServerDump> dumps = new ConcurrentHashMap<String,TRSServerDump>();
	

	public TRSDumpBolt(String fieldNames,final String databasePrefix,String timeout,String emitSize){
		this.fieldNames = StringUtil.parse(fieldNames);
		this.emitSize = Long.parseLong(emitSize);
		this.timeout = Long.parseLong(timeout);
		
		this.dbSelector = new DatabaseSelector(){
			private static final long serialVersionUID = -7541792663026508734L;
			@Override
			public String select(Tuple input) {
				String [] fieldValues = (String[])input.getValue(1);
				String time = fieldValues[6];
				String date = time.substring(0, DateUtil.DEFAULT_INPUT_FORMAT.length());
				String suffix = DateUtil.nextMonthFirstDay(date);
				return databasePrefix+suffix;
			}
		};
	}
	
	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		if(isTickTuple(input)){
			for(String db : dumps.keySet()){
				if(System.currentTimeMillis() - dumps.get(db).timestamp >= timeout){
					emit(collector,db);
				}
			}
		}else{
			String db = dbSelector.select(input);
			TRSServerDump dump = dumps.get(db);
			if(dump == null){
				try {
					dump = init(db);
				} catch (IOException e) {
					LOG.error("Fail to init dump file for "+db + ", which is selected by tuple"+input, e);
					collector.reportError(e);
					return;
				}
			}
			
			String [] fieldValues = (String[])input.getValue(1);
			try{
				dump.writer.append("<REC>").append("\n");
				for(int i = 0 ; i < fieldNames.length ; i++){
					dump.writer.append("<").append(fieldNames[i]).append(">").append("=").append(StringUtil.avoidNull(fieldValues[i])).append("\n");
				}
				dump.recordCount++;
			}catch (IOException e) {
				LOG.error("Fail to append record to "+dump, e);
				collector.reportError(e);
			}
			
			if(dump.recordCount >= emitSize){
				emit(collector,db);
			}
		}
	}
	
	TRSServerDump init(String db) throws IOException{
		File f = File.createTempFile(TRSDumpBolt.class.getName().toLowerCase()+"_"+db, ".trs");
		
		TRSServerDump dump = new TRSServerDump();
		dump.recordCount = 0;
		dump.timestamp = System.currentTimeMillis();
		dump.fileName = f.getCanonicalPath();
		dump.writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(f), "UTF8"));
		dumps.put(db, dump);
		return dump;
	}
	
	void emit(BasicOutputCollector collector,String db){
		TRSServerDump dump = dumps.remove(db);
		try {
			dump.writer.flush();
			dump.writer.close();
		} catch (IOException e) {
			LOG.error("Fail to flush and close "+dump.fileName + " , just ignore it, and then emit. may be lost something", e);
		}
		collector.emit(new Values(db,dump.fileName));
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("database","filename"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		Config conf = new Config();
		conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 10);
		return conf;
	}
	
    static boolean isTickTuple(Tuple tuple) {
        return tuple.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID)
            && tuple.getSourceStreamId().equals(Constants.SYSTEM_TICK_STREAM_ID);
    }
	
	@SuppressWarnings("serial")
	abstract public class DatabaseSelector implements Serializable{
		public abstract String select(Tuple input);
	}
    
	class TRSServerDump implements Serializable{

		private static final long serialVersionUID = -4027732151256333380L;

		/**
		 * 装库文件名
		 */
		String fileName;
		
		/**
		 * 创建时间
		 */
		long timestamp;
		
		/**
		 * 记录数
		 */
		long recordCount;
		
		/**
		 * 输出流
		 */
		Writer writer;
		
		public String toString(){
			return fileName+" at "+timestamp+" total "+recordCount;
		}
	}
}
