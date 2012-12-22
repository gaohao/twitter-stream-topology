package com.mrhooray.bolts;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.mrhooray.spouts.TwitterStreamSpout;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Transaction;
import twitter4j.Status;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

public class TopRetweetAlltimeBolt extends BaseBasicBolt {

	private static final long serialVersionUID = -5936036236747710453L;
	private static Logger logger = null;
	private static JedisPool pool = null;
	private final String key = "global:topretweeted:alltime";
	private int topCapacity = 0;

	@SuppressWarnings("static-access")
	public TopRetweetAlltimeBolt(String host, int port, int topCapacity) {
		this.pool = new JedisPool(host, port);
		this.logger = LogManager.getLogger(TwitterStreamSpout.class.getName());
		this.topCapacity = topCapacity;
	}

	@SuppressWarnings("static-access")
	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		Status status = (Status) input.getValue(0);
		long statusID = status.getId();
		long retweetCount = status.getRetweetCount();
		Jedis jedis = this.pool.getResource();
		if (jedis.zrank(key, String.valueOf(statusID)) != null) {
			jedis.zadd(key, (double) retweetCount, String.valueOf(statusID));
		} else {
			if (jedis.zcount(key, Double.MIN_VALUE, Double.MAX_VALUE) >= topCapacity) {
				redis.clients.jedis.Tuple t = (redis.clients.jedis.Tuple) jedis
						.zrangeWithScores(key, 0, 0).toArray()[0];
				double loweast = t.getScore();
				if (retweetCount > loweast) {
					jedis.watch(key);
					Transaction tran = jedis.multi();
					tran.zremrangeByRank(key, 0, 0);
					tran.zadd(key, (double) retweetCount,
							String.valueOf(statusID));
					tran.exec();
					jedis.unwatch();
				}
			} else {
				jedis.zadd(key, (double) retweetCount, String.valueOf(statusID));
			}
		}
		this.pool.returnResource(jedis);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

	@SuppressWarnings("static-access")
	@Override
	public void cleanup() {
		Jedis jedis = this.pool.getResource();
		for (redis.clients.jedis.Tuple t : jedis.zrangeWithScores(key, 0, -1)) {
			logger.info(t.getElement());
			logger.info(t.getScore());
		}
		this.pool.returnResource(jedis);
		this.pool.destroy();
	}
}
