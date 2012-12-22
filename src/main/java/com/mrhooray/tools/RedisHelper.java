package com.mrhooray.tools;

import java.io.Serializable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Transaction;
import twitter4j.Status;

public class RedisHelper implements Serializable {
	private static final long serialVersionUID = 165026396148159099L;
	private static Logger logger = LogManager.getLogger(RedisHelper.class
			.getName());;

	public static JedisPool getPool(String host, int port) {
		return new JedisPool(host, port);
	}

	public static void addToTopN(JedisPool pool, String key, long capacity,
			Status status) {
		String member = String.valueOf(status.getId());
		double score = (double) status.getRetweetCount();
		Jedis jedis = pool.getResource();
		jedis.watch(key);
		if (jedis.zrank(key, member) != null) {
			Transaction tran = jedis.multi();
			tran.zadd(key, (double) score, member);
			addStatus(tran, status);
			tran.exec();
		} else {
			if (jedis.zcount(key, -1, Double.MAX_VALUE) >= capacity) {
				redis.clients.jedis.Tuple t = (redis.clients.jedis.Tuple) jedis
						.zrangeWithScores(key, 0, 0).toArray()[0];
				double loweast = t.getScore();
				if (score > loweast) {
					Transaction tran = jedis.multi();
					tran.zremrangeByRank(key, 0, 0);
					removeStatus(tran, status);
					tran.zadd(key, (double) score, member);
					addStatus(tran, status);
					tran.exec();
				}
			} else {
				Transaction tran = jedis.multi();
				tran.zadd(key, (double) score, member);
				addStatus(tran, status);
				tran.exec();
			}
		}
		jedis.unwatch();
		pool.returnResource(jedis);
	}

	private static void addStatus(Transaction tran, Status status) {
		Gson gson = new Gson();
		tran.set("global:status:" + status.getId(), gson.toJson(status));
	}

	private static void removeStatus(Transaction tran, Status status) {
		tran.del("global:status:" + status.getId());
	}

	public static void destroy(JedisPool pool) {
		pool.destroy();
	}
}
