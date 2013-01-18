package com.mrhooray.topologies;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;

import com.mrhooray.bolts.DeleteTwitterPicBolt;
import com.mrhooray.bolts.FilterTweetBolt;
import com.mrhooray.bolts.ReapBolt;
import com.mrhooray.bolts.TopRetweetAllTimeBolt;
import com.mrhooray.bolts.TopRetweetShortPeriodBolt;
import com.mrhooray.bolts.IndexTwitterPicBolt;
import com.mrhooray.spouts.TimerSpout;
import com.mrhooray.spouts.TwitterStreamSpout;

public class TwitterStreamTopology {

	public static String consumerKey = null;
	public static String consumerSecret = null;
	public static String accessToken = null;
	public static String accessTokenSecret = null;
	public static String hostR = "localhost";
	public static String hostES = "localhost";
	public static int portR = 16379;
	public static int portES = 19300;
	public static int capacity = 100;
	public static double indexSizeInGB = 10;

	public static void main(String[] args) throws InterruptedException {
		parseArguments(args);

		TopologyBuilder builder = new TopologyBuilder();
		// spout
		builder.setSpout("tweets-spout", new TwitterStreamSpout(consumerKey,
				consumerSecret, accessToken, accessTokenSecret), 1);
		builder.setSpout("timer-spout", new TimerSpout(), 1);
		// bolt
		builder.setBolt("filter-tweet-bolt", new FilterTweetBolt(), 1)
				.shuffleGrouping("tweets-spout", "onstatus");
		builder.setBolt("delete-twitter-pic-bolt",
				new DeleteTwitterPicBolt(hostES, portES), 1).shuffleGrouping(
				"tweets-spout", "ondelete");
		builder.setBolt("top-retweet-alltime-bolt",
				new TopRetweetAllTimeBolt(hostR, portR, capacity), 2)
				.shuffleGrouping("filter-tweet-bolt", "alltime");
		builder.setBolt("top-retweet-shortperiod-bolt-24h",
				new TopRetweetShortPeriodBolt(hostR, portR, capacity, "24h"), 2)
				.shuffleGrouping("filter-tweet-bolt", "24h");
		builder.setBolt("top-retweet-shortperiod-bolt-1h",
				new TopRetweetShortPeriodBolt(hostR, portR, capacity, "1h"), 2)
				.shuffleGrouping("filter-tweet-bolt", "1h");
		builder.setBolt("top-retweet-shortperiod-bolt-1m",
				new TopRetweetShortPeriodBolt(hostR, portR, capacity, "1m"), 2)
				.shuffleGrouping("filter-tweet-bolt", "1m");
		builder.setBolt("index-twitter-pic-bolt",
				new IndexTwitterPicBolt(hostES, portES), 1).shuffleGrouping(
				"filter-tweet-bolt", "pic");
		builder.setBolt("reap-bolt",
				new ReapBolt(hostR, portR, hostES, portES, indexSizeInGB), 1)
				.shuffleGrouping("timer-spout");
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

	public static void parseArguments(String[] args) {
		Options opts = new Options();
		opts.addOption("h", "help", false, "show options");
		opts.addOption("s", "size", true,
				"max size of elasticsearch index in GB");
		opts.addOption("g", "gardenhose", false,
				"use gardenhose level of access");
		BasicParser parser = new BasicParser();
		CommandLine cl = null;
		try {
			cl = parser.parse(opts, args);
			if (cl.hasOption('h')) {
				HelpFormatter hf = new HelpFormatter();
				hf.printHelp("options", opts);
				System.exit(0);
			}
			if (cl.hasOption('s')) {
				indexSizeInGB = Double.parseDouble(cl.getOptionValue("s"));
			}
			if (cl.hasOption('g')) {
				// Garden Hose Access Level
				consumerKey = "ZbVRenq6eWoajY6XUZjrrQ";
				consumerSecret = "x5StK9aqX8LfFAUIKrAOeRVFb0autLF6rquNOVRLk";
				accessToken = "280466939-GH7xhmrK4CG89cJ8SeQtBTcOs0BNRzOACRL4DX5C";
				accessTokenSecret = "0k2Dfd8vJwjmAIkE34GeixnCrqIVoxoSg6DkHPpE";
			} else {
				// Default Access Level
				consumerKey = "KRUyRf2ILVsNiAQhGVkROw";
				consumerSecret = "y0H4D6p5bsvcZWti0MMsJywRn5HYnXXYvGs1dlQ15A";
				accessToken = "1068889405-a3CPN1s6HJK4nxgeIXfcDIPU5sVBbft3nr9Xvo";
				accessTokenSecret = "989IWliREBgkJVRglL0lZXDNpPf7tcI7PihY7sY";
			}
		} catch (ParseException e) {
			HelpFormatter hf = new HelpFormatter();
			hf.printHelp("options", opts);
			System.exit(0);
		} catch (NumberFormatException e) {
			System.out.println("argument error: size of index must be numeric");
			System.exit(0);
		}
	}
}
