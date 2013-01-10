package com.mrhooray.tools;

import java.io.IOException;
import java.io.Serializable;
import java.util.Date;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.admin.indices.stats.IndicesStats;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.sort.SortOrder;

import twitter4j.Status;

public class ElasticSearchHelper extends BaseHelper implements Serializable {
	private static final long serialVersionUID = 7332198408133535456L;
	private static final String index = "twitter";
	private static final String type = "tweet";

	public static Client getClient() {
		Settings settings = ImmutableSettings.settingsBuilder()
				.put("cluster.name", "twitter-pic-search").build();
		Client client = new TransportClient(settings)
				.addTransportAddress(new InetSocketTransportAddress(
						"localhost", 9300));
		IndicesExistsRequest ier = new IndicesExistsRequest(index);
		ActionFuture<IndicesExistsResponse> response = client.admin().indices()
				.exists(ier);
		if (response.actionGet().exists() == false) {
			client.admin().indices().prepareCreate(index).execute().actionGet();
		}
		XContentBuilder mapping = null;
		try {
			mapping = XContentFactory.jsonBuilder().startObject()
					.startObject(type).startObject("_source")
					.field("compress", true).endObject()
					.startObject("properties").startObject("text")
					.field("type", "string").field("store", "yes")
					.field("index", "analyzed").endObject().startObject("json")
					.field("type", "string").field("store", "yes")
					.field("index", "no").endObject().startObject("time")
					.field("type", "long").field("store", "yes")
					.field("index", "not_analyzed").endObject()
					.startObject("picurl").field("type", "string")
					.field("store", "yes").field("index", "not_analyzed")
					.endObject().endObject().endObject().endObject();
		} catch (IOException e) {
			e.printStackTrace();
		}
		PutMappingRequest mappingRequest = Requests.putMappingRequest(index)
				.type(type).source(mapping);
		client.admin().indices().putMapping(mappingRequest).actionGet();
		return client;
	}

	public static void index(Client client, Status status) {
		if (!isIndexed(client, status.getMediaEntities()[0].getMediaURL())) {
			try {
				XContentBuilder doc = XContentFactory
						.jsonBuilder()
						.startObject()
						.field("text", status.getText())
						.field("json", toJson(status))
						.field("time", status.getCreatedAt().getTime())
						.field("picurl",
								status.getMediaEntities()[0].getMediaURL())
						.endObject();
				client.prepareIndex(index, type).setSource(doc).execute()
						.actionGet();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	private static boolean isIndexed(Client client, String url) {
		QueryBuilder query = QueryBuilders.termQuery("picurl", url);
		SearchHits hits = client.prepareSearch(index).setQuery(query)
				.addSort("time", SortOrder.ASC).setSize(10).setExplain(true)
				.execute().actionGet().getHits();
		if (hits.totalHits() > 0) {
			return true;
		} else {
			return false;
		}
	}

	public static void reap(Client client, int sizeLimitInGB) {
		IndicesStats stats = client.admin().indices().prepareStats()
				.setIndices(index).execute().actionGet();
		double sizeInGB = stats.index(index).getTotal().getStore().getSize()
				.getGbFrac();
		if (sizeInGB > sizeLimitInGB) {
			QueryBuilder query = QueryBuilders.matchAllQuery();
			SearchHits hits = client.prepareSearch(index).setQuery(query)
					.addSort("time", SortOrder.ASC).setSize(10000)
					.setExplain(true).execute().actionGet().getHits();
			BulkRequestBuilder bulkRequest = client.prepareBulk();
			for (SearchHit hit : hits) {
				System.out
						.println(new Date((long) hit.getSource().get("time")));
				bulkRequest.add(client.prepareDelete(index, type, hit.getId())
						.setRefresh(true));
			}
			bulkRequest.execute().actionGet();
		}
	}

	public static void closeClient(Client client) {
		client.close();
	}
}
