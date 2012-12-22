package com.mrhooray.topologies;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;

import com.mrhooray.bolts.FilterRetweetBolt;
import com.mrhooray.bolts.TopRetweetAlltimeBolt;
import com.mrhooray.spouts.TwitterStreamSpout;

public class TwitterStreamTopology {

	public static void main(String[] args) throws InterruptedException {
		Logger logger = LogManager.getLogger(TwitterStreamTopology.class
				.getName());
		logger.entry();
		String consumerKey = "ZbVRenq6eWoajY6XUZjrrQ";
		String consumerSecret = "x5StK9aqX8LfFAUIKrAOeRVFb0autLF6rquNOVRLk";
		String accessToken = "280466939-GH7xhmrK4CG89cJ8SeQtBTcOs0BNRzOACRL4DX5C";
		String accessTokenSecret = "0k2Dfd8vJwjmAIkE34GeixnCrqIVoxoSg6DkHPpE";

		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("tweets-spout", new TwitterStreamSpout(consumerKey,
				consumerSecret, accessToken, accessTokenSecret));
		builder.setBolt("filter-retweet-bolt", new FilterRetweetBolt())
				.shuffleGrouping("tweets-spout");
		builder.setBolt("top-retweet-alltime-bolt",
				new TopRetweetAlltimeBolt("localhost", 6379, 10), 1)
				.shuffleGrouping("filter-retweet-bolt");

		Config conf = new Config();
		conf.setDebug(false);
		// conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("Twitter-Stream-Topology", conf,
				builder.createTopology());
		Thread.sleep(1000 * 60 * 1);
		cluster.shutdown();
		logger.exit();
	}
}
