package com.mrhooray.bolts;

import com.mrhooray.tools.RedisHelper;

import redis.clients.jedis.JedisPool;
import twitter4j.Status;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

public class TopRetweetShortPeriodBolt extends BaseBasicBolt {

	private static final long serialVersionUID = 8781711354150063405L;
	private static JedisPool pool = null;
	private long topCapacity = 0;
	private String prefix = null;

	@SuppressWarnings("static-access")
	public TopRetweetShortPeriodBolt(String host, int port, long topCapacity,
			String prefix) {
		this.pool = RedisHelper.getPool(host, port);
		this.topCapacity = topCapacity;
		this.prefix = prefix;
	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		Status status = (Status) input.getValue(0);
		RedisHelper.addToTopNShortPeriod(pool, topCapacity, status, prefix);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

	@Override
	public void cleanup() {
		RedisHelper.destroy(pool);
	}
}
