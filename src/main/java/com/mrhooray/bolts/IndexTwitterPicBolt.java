package com.mrhooray.bolts;

import org.elasticsearch.client.Client;

import com.mrhooray.tools.ElasticSearchHelper;

import twitter4j.Status;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

public class IndexTwitterPicBolt extends BaseBasicBolt {
	private static final long serialVersionUID = 1752377800313397267L;
	private static Client client = null;

	public IndexTwitterPicBolt(String host, int port) {
		client = ElasticSearchHelper.getClient(host, port);
	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		Status status = (Status) input.getValue(0);
		ElasticSearchHelper.index(client, status);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

	@Override
	public void cleanup() {
		ElasticSearchHelper.closeClient(client);
	}
}
