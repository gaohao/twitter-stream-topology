package com.mrhooray.bolts;

import com.mrhooray.tools.RedisHelper;

import redis.clients.jedis.JedisPool;
import twitter4j.Status;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

public class TopRetweetAlltimeBolt extends BaseBasicBolt {
	private static final long serialVersionUID = -5936036236747710453L;
	private static JedisPool pool = null;
	private long topCapacity = 0;

	@SuppressWarnings("static-access")
	public TopRetweetAlltimeBolt(String host, int port, long topCapacity) {
		this.pool = RedisHelper.getPool(host, port);
		this.topCapacity = topCapacity;
	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		Status status = (Status) input.getValue(0);
		RedisHelper.addToTopNAlltime(pool, topCapacity, status);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

	@Override
	public void cleanup() {
		RedisHelper.destroy(pool);
	}
}
