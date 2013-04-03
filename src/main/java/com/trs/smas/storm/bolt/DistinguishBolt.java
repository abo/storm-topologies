package com.trs.smas.storm.bolt;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

/**
 * 排除重复的内容
 * @author huangshengbo
 *
 */
public class DistinguishBolt extends BaseBasicBolt {

	private static final long serialVersionUID = -8263138164437539555L;
	
	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
//		String topic = input.getString(0);
//		String [] fields = (String[])input.getValue(1);
//		// TODO Auto-generated method stub

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("topic","fields"));
	}
}
