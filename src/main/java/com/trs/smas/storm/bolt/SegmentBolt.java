package com.trs.smas.storm.bolt;

import java.util.Map;

import backtype.storm.task.ShellBolt;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;

public class SegmentBolt extends ShellBolt implements IRichBolt {

	private static final long serialVersionUID = 6550405408415435434L;

	public SegmentBolt() {
		super("python", "segment.py");
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

}
