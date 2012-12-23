package com.mrhooray.bolts;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

public class ReapBolt extends BaseBasicBolt {
	
	private static final long serialVersionUID = 6069215708986553477L;

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		
	}
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		
	}
}
