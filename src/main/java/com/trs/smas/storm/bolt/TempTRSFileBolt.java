package com.trs.smas.storm.bolt;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.Map;

import backtype.storm.Config;
import backtype.storm.Constants;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.trs.smas.storm.util.CSVUtil;

public class TempTRSFileBolt extends BaseBasicBolt {

	private static final long serialVersionUID = -4481643466309672632L;
	
	private String [] fieldNames;
	private String database;
	Writer out;
	String tmpFileName;
	int recordCount;
	long timestamp;

	public TempTRSFileBolt(String fieldNames,String database){
		this.fieldNames = CSVUtil.parse(fieldNames);
		this.database = database;
	}
	
	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		if(!isTickTuple(input)){
			if(out == null){
				try {
					createTempFile();
				} catch (IOException e) {
					e.printStackTrace();
					collector.reportError(e);
				}
			}
			String [] fieldValues = (String []) input.getValue(1);
			try {
				out.append("<REC>").append("\n");
				for(int i = 0 ; i < fieldNames.length ; i ++ ){
					out.append("<").append(fieldNames[i]).append(">").append("=").append(fieldValues[i]).append("\n");
				}
				recordCount++;
			} catch (IOException e) {
				e.printStackTrace();
				collector.reportError(e);
			}
		}
		
		if(fillOrTimeout()){
			try {
				emitToLoad(collector);
			} catch (IOException e) {
				e.printStackTrace();
				collector.reportError(e);
			}
		}
	}
	
	
	private boolean fillOrTimeout(){
		return (recordCount >= 10000) || ((recordCount > 0) && (System.currentTimeMillis() - timestamp >= 3*60*1000));
	}
	
	private void emitToLoad(BasicOutputCollector collector) throws IOException{
		out.flush();
		out.close();
		out=null;
		collector.emit(new Values(database,tmpFileName));
		recordCount = 0;
		tmpFileName = null;
	}
	
	private void createTempFile() throws IOException{
		File f = File.createTempFile("com.trs.smas.storm.", ".trs");
		tmpFileName = f.getAbsolutePath().replace("\\", "\\\\");
		
		out = new BufferedWriter(new OutputStreamWriter(
				new FileOutputStream(f), "UTF8"));
		
		recordCount=0;
		timestamp = System.currentTimeMillis();
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
	
    private static boolean isTickTuple(Tuple tuple) {
        return tuple.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID)
            && tuple.getSourceStreamId().equals(Constants.SYSTEM_TICK_STREAM_ID);
    }
}
