package com.mrhooray.tools;

import java.io.Serializable;
import java.util.Set;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Transaction;
import twitter4j.Status;

public class RedisHelper extends BaseHelper implements Serializable {
	private static final long serialVersionUID = 165026396148159099L;

	public static JedisPool getPool(String host, int port) {
		return new JedisPool(host, port);
	}

	public static void addToTopNAlltime(JedisPool pool, long capacity,
			Status status) {
		String prefix = "alltime";
		String key = "topretweet:" + prefix + ":count";
		String member = String.valueOf(status.getId());
		double score = (double) status.getRetweetCount();
		Jedis jedis = pool.getResource();
		jedis.watch(key);
		if (jedis.zrank(key, member) != null) {
			Transaction tran = jedis.multi();
			tran.zadd(key, score, member);
			addStatus(tran, prefix, status);
			tran.exec();
		} else {
			if (jedis.zcount(key, -1, Double.MAX_VALUE) >= capacity) {
				redis.clients.jedis.Tuple t = (redis.clients.jedis.Tuple) jedis
						.zrangeWithScores(key, 0, 0).toArray()[0];
				double loweast = t.getScore();
				String loweast_member = (String) jedis.zrange(key, 0, 0)
						.toArray()[0];
				if (score > loweast) {
					Transaction tran = jedis.multi();
					tran.zrem(key, loweast_member);
					removeStatus(tran, prefix, loweast_member);
					tran.zadd(key, score, member);
					addStatus(tran, prefix, status);
					tran.exec();
				}
			} else {
				Transaction tran = jedis.multi();
				tran.zadd(key, (double) score, member);
				addStatus(tran, prefix, status);
				tran.exec();
			}
		}
		jedis.unwatch();
		pool.returnResource(jedis);
	}

	public static void addToTopNShortPeriod(JedisPool pool, long capacity,
			Status status, String prefix) {
		String keyByCount = "topretweet:" + prefix + ":count";
		String keyByTime = "topretweet:" + prefix + ":time";
		String member = String.valueOf(status.getId());
		double count = (double) status.getRetweetCount();
		double time = (double) status.getCreatedAt().getTime();
		Jedis jedis = pool.getResource();
		jedis.watch(keyByCount, keyByTime);
		if (jedis.zrank(keyByCount, member) != null) {
			Transaction tran = jedis.multi();
			tran.zadd(keyByCount, count, member);
			tran.zadd(keyByTime, time, member);
			addStatus(tran, prefix, status);
			tran.exec();
		} else {
			if (jedis.zcount(keyByCount, -1, Double.MAX_VALUE) >= capacity) {
				redis.clients.jedis.Tuple t = (redis.clients.jedis.Tuple) jedis
						.zrangeWithScores(keyByCount, 0, 0).toArray()[0];
				double loweast = t.getScore();
				String loweast_member = (String) jedis.zrange(keyByCount, 0, 0)
						.toArray()[0];
				if (count > loweast) {
					Transaction tran = jedis.multi();
					tran.zrem(keyByCount, loweast_member);
					tran.zrem(keyByTime, loweast_member);
					removeStatus(tran, prefix, loweast_member);
					tran.zadd(keyByCount, (double) count, member);
					tran.zadd(keyByTime, time, member);
					addStatus(tran, prefix, status);
					tran.exec();
				}
			} else {
				Transaction tran = jedis.multi();
				tran.zadd(keyByCount, (double) count, member);
				tran.zadd(keyByTime, time, member);
				addStatus(tran, prefix, status);
				tran.exec();
			}
		}
		jedis.unwatch();
		pool.returnResource(jedis);
	}

	public static void reap(JedisPool pool, long shortPeriod, String prefix) {
		String keyByCount = "topretweet:" + prefix + ":count";
		String keyByTime = "topretweet:" + prefix + ":time";
		Jedis jedis = pool.getResource();
		jedis.watch(keyByCount, keyByTime);
		Set<String> past = jedis.zrangeByScore(keyByTime, 0,
				System.currentTimeMillis() - shortPeriod);
		for (String str : past) {
			Transaction tran = jedis.multi();
			tran.zrem(keyByTime, str);
			tran.zrem(keyByCount, str);
			removeStatus(tran, prefix, str);
			tran.exec();
		}
		jedis.unwatch();
		pool.returnResource(jedis);
	}

	private static void addStatus(Transaction tran, String prefix, Status status) {
		tran.set("tweet:" + prefix + ":" + status.getId(), toJson(status));
	}

	private static void removeStatus(Transaction tran, String prefix, String id) {
		tran.del("tweet:" + prefix + ":" + id);
	}

	public static void destroy(JedisPool pool) {
		pool.destroy();
	}
}
