package com.mrhooray.bolts;

import com.mrhooray.tools.ElasticSearchHelper;

import io.searchbox.client.JestClient;
import twitter4j.Status;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

public class IndexTwitterPicBolt extends BaseBasicBolt {
	private static final long serialVersionUID = 1752377800313397267L;
	private static JestClient client = null;

	public IndexTwitterPicBolt() {
		client = ElasticSearchHelper.getClient();
	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		Status status = (Status) input.getValue(0);
		System.out.println(status.getMediaEntities()[0].getMediaURL());
		ElasticSearchHelper.add(client, status);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

	@Override
	public void cleanup() {
		ElasticSearchHelper.shutdownClient(client);
	}
}
