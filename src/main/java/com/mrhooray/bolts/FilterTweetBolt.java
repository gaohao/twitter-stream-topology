package com.mrhooray.bolts;

import twitter4j.Status;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class FilterTweetBolt extends BaseBasicBolt {
	private static final long serialVersionUID = 6069215708986553477L;
	private long h_24 = 86400000;
	private long h_1 = 3600000;
	private long m_1 = 60000;

	public FilterTweetBolt() {
	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		Status status = (Status) input.getValue(0);
		if (status.isRetweet()) {
			collector.emit("alltime", new Values(status.getRetweetedStatus()));
			if (System.currentTimeMillis()
					- status.getRetweetedStatus().getCreatedAt().getTime() <= h_24) {
				collector.emit("24h", new Values(status.getRetweetedStatus()));
				if (System.currentTimeMillis()
						- status.getRetweetedStatus().getCreatedAt().getTime() <= h_1) {
					collector.emit("1h",
							new Values(status.getRetweetedStatus()));
					if (System.currentTimeMillis()
							- status.getRetweetedStatus().getCreatedAt()
									.getTime() <= m_1) {
						collector.emit("1m",
								new Values(status.getRetweetedStatus()));
					}
				}
			}
		}
		if (status.getMediaEntities().length > 0) {
			collector.emit("pic", new Values(status));
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream("alltime",
				new Fields("retweeted-status-alltime"));
		declarer.declareStream("24h", new Fields("retweeted-status-24h"));
		declarer.declareStream("1h", new Fields("retweeted-status-1h"));
		declarer.declareStream("1m", new Fields("retweeted-status-1m"));
		declarer.declareStream("pic", new Fields("status-pic"));
	}
}
