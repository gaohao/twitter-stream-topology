package com.mrhooray.topologies;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;

import com.mrhooray.bolts.FilterTweetBolt;
import com.mrhooray.bolts.ReapBolt;
import com.mrhooray.bolts.TopRetweetAllTimeBolt;
import com.mrhooray.bolts.TopRetweetShortPeriodBolt;
import com.mrhooray.bolts.IndexTwitterPicBolt;
import com.mrhooray.spouts.TimerSpout;
import com.mrhooray.spouts.TwitterStreamSpout;

public class TwitterStreamTopology {

	public static void main(String[] args) throws InterruptedException {
		// Default Level
		String consumerKey = "KRUyRf2ILVsNiAQhGVkROw";
		String consumerSecret = "y0H4D6p5bsvcZWti0MMsJywRn5HYnXXYvGs1dlQ15A";
		String accessToken = "1068889405-a3CPN1s6HJK4nxgeIXfcDIPU5sVBbft3nr9Xvo";
		String accessTokenSecret = "989IWliREBgkJVRglL0lZXDNpPf7tcI7PihY7sY";
		// Garden Hose
		// String consumerKey = "ZbVRenq6eWoajY6XUZjrrQ";
		// String consumerSecret = "x5StK9aqX8LfFAUIKrAOeRVFb0autLF6rquNOVRLk";
		// String accessToken =
		// "280466939-GH7xhmrK4CG89cJ8SeQtBTcOs0BNRzOACRL4DX5C";
		// String accessTokenSecret =
		// "0k2Dfd8vJwjmAIkE34GeixnCrqIVoxoSg6DkHPpE";
		String host = "localhost";
		int port = 6379;
		int capacity = 100;

		TopologyBuilder builder = new TopologyBuilder();
		// spout
		builder.setSpout("tweets-spout", new TwitterStreamSpout(consumerKey,
				consumerSecret, accessToken, accessTokenSecret), 1);
		builder.setSpout("timer-spout", new TimerSpout(), 1);
		// bolt
		builder.setBolt("filter-tweet-bolt", new FilterTweetBolt(), 1)
				.shuffleGrouping("tweets-spout");
		builder.setBolt("top-retweet-alltime-bolt",
				new TopRetweetAllTimeBolt(host, port, capacity), 2)
				.shuffleGrouping("filter-tweet-bolt", "alltime");
		builder.setBolt("top-retweet-shortperiod-bolt-24h",
				new TopRetweetShortPeriodBolt(host, port, capacity, "24h"), 2)
				.shuffleGrouping("filter-tweet-bolt", "24h");
		builder.setBolt("top-retweet-shortperiod-bolt-1h",
				new TopRetweetShortPeriodBolt(host, port, capacity, "1h"), 2)
				.shuffleGrouping("filter-tweet-bolt", "1h");
		builder.setBolt("top-retweet-shortperiod-bolt-1m",
				new TopRetweetShortPeriodBolt(host, port, capacity, "1m"), 2)
				.shuffleGrouping("filter-tweet-bolt", "1m");
		builder.setBolt("twitter-pic-index-bolt", new IndexTwitterPicBolt(), 1)
				.shuffleGrouping("filter-tweet-bolt", "pic");
		builder.setBolt("reap-bolt", new ReapBolt(host, port)).shuffleGrouping(
				"timer-spout");
		// configure and submit
		Config conf = new Config();
		conf.setDebug(false);
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("Twitter-Stream-Topology", conf,
				builder.createTopology());
		// for development
		// Thread.sleep(1000 * 60 * 60 * 2);
		// cluster.shutdown();
	}
}
