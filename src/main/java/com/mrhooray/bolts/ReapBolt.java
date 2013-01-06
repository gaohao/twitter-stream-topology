package com.mrhooray.bolts;

import com.mrhooray.tools.RedisHelper;

import redis.clients.jedis.JedisPool;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

public class ReapBolt extends BaseBasicBolt {
	private static final long serialVersionUID = 6069215708986553477L;
	private static JedisPool pool = null;
	private long periodTime = 0;

	@SuppressWarnings("static-access")
	public ReapBolt(String host, int port, long periodTime) {
		this.pool = RedisHelper.getPool(host, port);
		this.periodTime = periodTime;
	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		RedisHelper.reap(pool, periodTime);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

	@Override
	public void cleanup() {
		RedisHelper.destroy(pool);
	}
}
