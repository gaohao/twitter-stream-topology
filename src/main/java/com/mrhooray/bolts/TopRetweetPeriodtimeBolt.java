package com.mrhooray.bolts;

import com.mrhooray.tools.RedisHelper;

import redis.clients.jedis.JedisPool;
import twitter4j.Status;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

public class TopRetweetPeriodtimeBolt extends BaseBasicBolt {

	private static final long serialVersionUID = 8781711354150063405L;
	private static JedisPool pool = null;
	private long topCapacity = 0;

	@SuppressWarnings("static-access")
	public TopRetweetPeriodtimeBolt(String host, int port, long topCapacity) {
		this.pool = RedisHelper.getPool(host, port);
		this.topCapacity = topCapacity;
	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		Status status = (Status) input.getValue(0);
		RedisHelper.addToTopNPeriodtime(pool, topCapacity, status);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

	@Override
	public void cleanup() {
		RedisHelper.destroy(pool);
	}
}
