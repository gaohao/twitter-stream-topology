package com.mrhooray.bolts;

import twitter4j.Status;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class FilterRetweetBolt extends BaseBasicBolt {
	private static final long serialVersionUID = 6069215708986553477L;
	private long periodTime = 0;

	public FilterRetweetBolt(long periodTime) {
		this.periodTime = periodTime;
	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		Status status = (Status) input.getValue(0);
		if (status.isRetweet()) {
			collector.emit("alltime", new Values(status.getRetweetedStatus()));
			if (System.currentTimeMillis()
					- status.getRetweetedStatus().getCreatedAt().getTime() <= periodTime) {
				collector.emit("periodtime",
						new Values(status.getRetweetedStatus()));
			}
		} else {
			return;
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream("alltime",
				new Fields("retweeted-status-alltime"));
		declarer.declareStream("periodtime", new Fields(
				"retweeted-status-periodtime"));
	}
}
