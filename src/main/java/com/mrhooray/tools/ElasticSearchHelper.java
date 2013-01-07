package com.mrhooray.tools;

import java.io.IOException;
import java.io.Serializable;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import twitter4j.Status;

public class ElasticSearchHelper extends BaseHelper implements Serializable {
	private static final long serialVersionUID = 7332198408133535456L;

	public static Client getClient() {
		Settings settings = ImmutableSettings.settingsBuilder()
				.put("cluster.name", "twitter-pic-search").build();
		Client client = new TransportClient(settings)
				.addTransportAddress(new InetSocketTransportAddress(
						"localhost", 9300));
		IndicesExistsRequest ier = new IndicesExistsRequest("twitter");
		ActionFuture<IndicesExistsResponse> response = client.admin().indices()
				.exists(ier);
		if (response.actionGet().exists() == false) {
			client.admin().indices().prepareCreate("twitter").execute()
					.actionGet();
		}
		XContentBuilder mapping = null;
		try {
			mapping = XContentFactory.jsonBuilder().startObject()
					.startObject("tweet").startObject("properties")
					.startObject("text").field("type", "string")
					.field("store", "yes").field("index", "analyzed")
					.endObject().startObject("json").field("type", "string")
					.field("store", "yes").field("index", "not_analyzed")
					.endObject().startObject("createdAt").field("type", "long")
					.field("store", "yes").field("index", "not_analyzed")
					.endObject().endObject().endObject().endObject();
		} catch (IOException e) {
			e.printStackTrace();
		}
		PutMappingRequest mappingRequest = Requests
				.putMappingRequest("twitter").type("tweet").source(mapping);
		client.admin().indices().putMapping(mappingRequest).actionGet();
		return client;
	}

	public static void index(Client client, Status status) {
		try {
			XContentBuilder doc = XContentFactory.jsonBuilder().startObject()
					.field("text", status.getText())
					.field("json", toJson(status))
					.field("time", status.getCreatedAt().getTime())
					.endObject();
			client.prepareIndex("twitter", "tweet").setSource(doc).execute()
					.actionGet();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void reap() {

	}

	public static void closeClient(Client client) {
		client.close();
	}
}
