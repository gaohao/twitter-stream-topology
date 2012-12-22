package com.mrhooray.spouts;

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.ConfigurationBuilder;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class TwitterStreamSpout extends BaseRichSpout {
	private static final long serialVersionUID = 5173509952980902144L;
	private LinkedBlockingQueue<Status> queue = null;
	private final int queueCapacity = 1024;
	private SpoutOutputCollector collector = null;
	private TwitterStream twitterStream = null;
	private String consumerKey = null;
	private String consumerSecret = null;
	private String accessToken = null;
	private String accessTokenSecret = null;
	private Logger logger = null;

	public TwitterStreamSpout(String consumerKey, String consumerSecret,
			String accessToken, String accessTokenSecret) {
		this.consumerKey = consumerKey;
		this.consumerSecret = consumerSecret;
		this.accessToken = accessToken;
		this.accessTokenSecret = accessTokenSecret;
		this.queue = new LinkedBlockingQueue<>(this.queueCapacity);
		this.logger = LogManager.getLogger(TwitterStreamSpout.class.getName());
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		ConfigurationBuilder builder = new ConfigurationBuilder();
		builder.setOAuthConsumerKey(this.consumerKey);
		builder.setOAuthConsumerSecret(this.consumerSecret);
		builder.setOAuthAccessToken(this.accessToken);
		builder.setOAuthAccessTokenSecret(this.accessTokenSecret);
		TwitterStreamFactory factory = new TwitterStreamFactory(builder.build());
		StatusListener listener = new StatusListener() {
			@Override
			public void onException(Exception ex) {
			}

			@Override
			public void onDeletionNotice(
					StatusDeletionNotice statusDeletionNotice) {
			}

			@Override
			public void onScrubGeo(long userId, long upToStatusId) {
			}

			@Override
			public void onStallWarning(StallWarning warning) {
			}

			@Override
			public void onStatus(Status status) {
				queue.offer(status);
			}

			@Override
			public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
				logger.info("onTrackLimitationNotice: "
						+ numberOfLimitedStatuses);
			}

		};
		this.twitterStream = factory.getInstance();
		this.twitterStream.addListener(listener);
		this.collector = collector;
		this.twitterStream.sample();
	}

	@Override
	public void nextTuple() {
		Status status = queue.poll();
		if (status == null) {
			Utils.sleep(20);
		} else {
			this.collector.emit(new Values(status));
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("status"));
	}

	@Override
	public void close() {
		this.twitterStream.cleanUp();
	}
}
