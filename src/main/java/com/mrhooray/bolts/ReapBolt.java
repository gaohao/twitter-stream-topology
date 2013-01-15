package com.mrhooray.bolts;

import org.elasticsearch.client.Client;

import com.mrhooray.tools.ElasticSearchHelper;
import com.mrhooray.tools.RedisHelper;

import redis.clients.jedis.JedisPool;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

public class ReapBolt extends BaseBasicBolt {
	private static final long serialVersionUID = 6069215708986553477L;
	private static JedisPool pool = null;
	private static Client client = null;
	private long h_24 = 86400000;
	private long h_1 = 3600000;
	private long m_1 = 60000;

	public ReapBolt(String hostR, int portR, String hostES, int portES) {
		pool = RedisHelper.getPool(hostR, portR);
		client = ElasticSearchHelper.getClient(hostES, portES);
	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		RedisHelper.reap(pool, h_24, "24h");
		RedisHelper.reap(pool, h_1, "1h");
		RedisHelper.reap(pool, m_1, "1m");
		ElasticSearchHelper.reap(client, 5);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

	@Override
	public void cleanup() {
		RedisHelper.destroy(pool);
		ElasticSearchHelper.closeClient(client);
	}
}
