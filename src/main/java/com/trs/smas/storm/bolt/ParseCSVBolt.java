package com.trs.smas.storm.bolt;

import java.io.IOException;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.trs.smas.storm.util.CSVUtil;

/**
 * 解析CSV格式的行
 * @author huangshengbo
 *
 */
public class ParseCSVBolt extends BaseBasicBolt {

	private static final long serialVersionUID = 1703468659718068776L;
	
	public ParseCSVBolt(){
	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		String [] fields;
		try {
			fields = CSVUtil.parse(input.getString(1));
		} catch (IOException e) {
			collector.reportError(e);
			return;
		}
		
		collector.emit(new Values(input.getString(0),fields));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("topic","fields"));
	}
}
