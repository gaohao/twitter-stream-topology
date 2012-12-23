package com.mrhooray.tools;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Transaction;
import twitter4j.Status;

public class RedisHelper implements Serializable {
	private static final long serialVersionUID = 165026396148159099L;

	public static JedisPool getPool(String host, int port) {
		return new JedisPool(host, port);
	}

	public static void addToTopN(JedisPool pool, String prefix, long capacity,
			Status status) {
		String key = "global:topretweet:" + prefix;
		String member = String.valueOf(status.getId());
		double score = (double) status.getRetweetCount();
		Jedis jedis = pool.getResource();
		jedis.watch(key);
		if (jedis.zrank(key, member) != null) {
			Transaction tran = jedis.multi();
			tran.zadd(key, (double) score, member);
			addStatus(tran, prefix, status);
			tran.exec();
		} else {
			if (jedis.zcount(key, -1, Double.MAX_VALUE) >= capacity) {
				redis.clients.jedis.Tuple t = (redis.clients.jedis.Tuple) jedis
						.zrangeWithScores(key, 0, 0).toArray()[0];
				double loweast = t.getScore();
				if (score > loweast) {
					Transaction tran = jedis.multi();
					removeStatus(tran, prefix, String.valueOf(status.getId()));
					tran.zremrangeByRank(key, 0, 0);
					tran.zadd(key, (double) score, member);
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

	private static void addStatus(Transaction tran, String prefix, Status status) {
		Gson gson = new Gson();
		JsonObject json = (JsonObject) gson.toJsonTree(status);
		json.remove("createdAt");
		json.addProperty("createdAt", getUTC(status.getCreatedAt()));
		tran.set("global:status:" + prefix + ":" + status.getId(),
				json.toString());
	}

	private static void removeStatus(Transaction tran, String prefix, String id) {
		tran.del("global:status:" + prefix + ":" + id);
	}

	private static String getUTC(Date date) {
		String ISO_FORMAT = "yyyy-MM-dd'T'HH:mm:ss zzz";
		SimpleDateFormat sdf = new SimpleDateFormat(ISO_FORMAT);
		TimeZone utc = TimeZone.getTimeZone("UTC");
		sdf.setTimeZone(utc);
		return sdf.format(date);
	}

	public static void destroy(JedisPool pool) {
		pool.destroy();
	}
}
