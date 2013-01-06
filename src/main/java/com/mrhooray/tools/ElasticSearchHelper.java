package com.mrhooray.tools;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.ClientConfig;
import io.searchbox.client.config.ClientConstants;

import java.io.Serializable;
import java.util.LinkedHashSet;

import twitter4j.Status;

public class ElasticSearchHelper implements Serializable {
	private static final long serialVersionUID = 7332198408133535456L;

	public static JestClient getClient() {
		// Configuration
		ClientConfig clientConfig = new ClientConfig();
		LinkedHashSet<String> servers = new LinkedHashSet<String>();
		servers.add("http://localhost:9200");
		clientConfig.getServerProperties().put(ClientConstants.SERVER_LIST,
				servers);
		// Construct a new Jest client according to configuration via factory
		JestClientFactory factory = new JestClientFactory();
		factory.setClientConfig(clientConfig);
		JestClient client = factory.getObject();
		return client;
	}

	public static void add(JestClient client, Status status) {

	}

	public static void shutdownClient(JestClient client) {
		// client.shutdownClient();
	}
}
