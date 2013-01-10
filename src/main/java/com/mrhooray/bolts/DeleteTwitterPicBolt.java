package com.mrhooray.bolts;

import org.elasticsearch.client.Client;

import com.mrhooray.tools.ElasticSearchHelper;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

public class DeleteTwitterPicBolt extends BaseBasicBolt {
	private static final long serialVersionUID = -8031426307137555777L;
	private static Client client = null;

	public DeleteTwitterPicBolt() {
		client = ElasticSearchHelper.getClient();
	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		long deletedStatusID = (long) input.getValue(0);
		ElasticSearchHelper.delete(client, deletedStatusID);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

	@Override
	public void cleanup() {
		ElasticSearchHelper.closeClient(client);
	}

}
