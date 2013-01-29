package com.mrhooray.topologies;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

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

	private static String consumerKey = null;
	private static String consumerSecret = null;
	private static String accessToken = null;
	private static String accessTokenSecret = null;
	private static String hostRedis = null;
	private static String hostES = null;
	private static int portRedis = -1;
	private static int portES = -1;
	private static boolean isGardenHose = false;
	private static int capacity = -1;
	private static double indexSizeInGB = -1;

	public static void main(String[] args) {
		parseArguments(args);
		parseConfig();

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
				new TopRetweetAllTimeBolt(hostRedis, portRedis, capacity), 2)
				.shuffleGrouping("filter-tweet-bolt", "alltime");
		builder.setBolt(
				"top-retweet-shortperiod-bolt-24h",
				new TopRetweetShortPeriodBolt(hostRedis, portRedis, capacity,
						"24h"), 2).shuffleGrouping("filter-tweet-bolt", "24h");
		builder.setBolt(
				"top-retweet-shortperiod-bolt-1h",
				new TopRetweetShortPeriodBolt(hostRedis, portRedis, capacity,
						"1h"), 2).shuffleGrouping("filter-tweet-bolt", "1h");
		builder.setBolt(
				"top-retweet-shortperiod-bolt-1m",
				new TopRetweetShortPeriodBolt(hostRedis, portRedis, capacity,
						"1m"), 2).shuffleGrouping("filter-tweet-bolt", "1m");
		builder.setBolt("index-twitter-pic-bolt",
				new IndexTwitterPicBolt(hostES, portES), 1).shuffleGrouping(
				"filter-tweet-bolt", "pic");
		builder.setBolt(
				"reap-bolt",
				new ReapBolt(hostRedis, portRedis, hostES, portES,
						indexSizeInGB), 1).shuffleGrouping("timer-spout");
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

	private static void parseArguments(String[] args) {
		Options opts = new Options();
		opts.addOption("h", "help", false, "show options");
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
			if (cl.hasOption('g')) {
				isGardenHose = true;
			} else {
				isGardenHose = false;
			}
		} catch (ParseException e) {
			HelpFormatter hf = new HelpFormatter();
			hf.printHelp("options", opts);
			System.exit(0);
		}
	}

	private static void parseConfig() {
		Properties prop = new Properties();
		try {
			InputStream in = new FileInputStream("config.properties");
			prop.load(in);
			if (isGardenHose) {
				consumerKey = prop.getProperty("consumerKeyGardenHose");
				consumerSecret = prop.getProperty("consumerSecretGardenHose");
				accessToken = prop.getProperty("accessTokenGardenHose");
				accessTokenSecret = prop
						.getProperty("accessTokenSecretGardenHose");
			}
			consumerKey = prop.getProperty("consumerKey");
			consumerSecret = prop.getProperty("consumerSecret");
			accessToken = prop.getProperty("accessToken");
			accessTokenSecret = prop.getProperty("accessTokenSecret");

			hostRedis = prop.getProperty("hostRedis");
			hostES = prop.getProperty("hostES");
			portRedis = Integer.valueOf(prop.getProperty("portRedis"));
			portES = Integer.valueOf(prop.getProperty("portES"));
			capacity = Integer.valueOf(prop.getProperty("capacity"));
			indexSizeInGB = Double.valueOf(prop.getProperty("indexSizeInGB"));
			in.close();
		} catch (IOException e) {
			System.out.println("Missing config file.");
			System.exit(0);
		} catch (Exception e) {
			System.out.println("Error when parsing config file.");
			System.exit(0);
		}

	}
}
