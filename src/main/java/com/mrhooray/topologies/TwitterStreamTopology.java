package com.mrhooray.topologies;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;

import com.mrhooray.bolts.FilterRetweetBolt;
import com.mrhooray.bolts.ReapBolt;
import com.mrhooray.bolts.TopRetweetAllTimeBolt;
import com.mrhooray.bolts.TopRetweetShortPeriodBolt;
import com.mrhooray.spouts.TimerSpout;
import com.mrhooray.spouts.TwitterStreamSpout;

public class TwitterStreamTopology {

	public static void main(String[] args) throws InterruptedException {
		String consumerKey = "ZbVRenq6eWoajY6XUZjrrQ";
		String consumerSecret = "x5StK9aqX8LfFAUIKrAOeRVFb0autLF6rquNOVRLk";
		String accessToken = "280466939-GH7xhmrK4CG89cJ8SeQtBTcOs0BNRzOACRL4DX5C";
		String accessTokenSecret = "0k2Dfd8vJwjmAIkE34GeixnCrqIVoxoSg6DkHPpE";
		String host = "localhost";
		int port = 6379;
		long shortPeriod = 86400000;
		int capacity = 100;

		TopologyBuilder builder = new TopologyBuilder();
		// spout
		builder.setSpout("tweets-spout", new TwitterStreamSpout(consumerKey,
				consumerSecret, accessToken, accessTokenSecret), 1);
		builder.setSpout("timer-spout", new TimerSpout(), 1);
		// bolt
		builder.setBolt("filter-retweet-bolt",
				new FilterRetweetBolt(shortPeriod), 1).shuffleGrouping(
				"tweets-spout");
		builder.setBolt("top-retweet-alltime-bolt",
				new TopRetweetAllTimeBolt(host, port, capacity), 2)
				.shuffleGrouping("filter-retweet-bolt", "alltime");
		builder.setBolt("top-retweet-shortperiod-bolt",
				new TopRetweetShortPeriodBolt(host, port, capacity, "24h"), 2)
				.shuffleGrouping("filter-retweet-bolt", "24h");
		builder.setBolt("reap-bolt", new ReapBolt(host, port, shortPeriod))
				.shuffleGrouping("timer-spout");
		// configure and submit
		Config conf = new Config();
		conf.setDebug(false);
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("Twitter-Stream-Topology", conf,
				builder.createTopology());
		// for development
		Thread.sleep(1000 * 60 * 60 * 24);
		cluster.shutdown();
	}
}
