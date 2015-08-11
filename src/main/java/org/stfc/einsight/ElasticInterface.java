package org.stfc.einsight;

import java.util.*;

import org.elasticsearch.transport.*;
import org.elasticsearch.client.transport.*;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.transport.*;
import org.elasticsearch.common.settings.*;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.search.*;

public class ElasticInterface {
    private Client client;

    private String searchQuery = "{" +
        "\"match\" : {" +
            "\"castor_MSG\" : \"New Request Arrival\"" +
        "}" +
    "}";

    ElasticInterface() {
    }

    ElasticInterface(String hostname, String clusterName) {
        connect(hostname, clusterName);
    }

    public void connect(String hostname, String clusterName) {
        // Specify the cluster name
        Settings settings = ImmutableSettings.settingsBuilder()
            .put("client.transport.sniff", true)
            .put("cluster.name", clusterName).build();

        client = new TransportClient(settings)
            .addTransportAddress(new InetSocketTransportAddress(hostname, 9300));
    }

    public void runLoadQuery() {
        String index = "logstash-2015.08.11";

        MagdbInterface magdbInt = new MagdbInterface();
        magdbInt.connect();
        magdbInt.runLoadQuery();
        ArrayList machineList = magdbInt.getMachineList();

        SearchResponse response = client.prepareSearch(index)
            .setTypes("castor")
            .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
            .setScroll(new TimeValue(60000))
            .setQuery(searchQuery)
            .setSize(100)
            .execute()
            .actionGet();

        for (int i = 0; i < response.getHits().getHits().length; i++) {
            SearchHit curHit = response.getHits().getHits()[i];
            Map<String, Object> data = curHit.sourceAsMap();
        }
    }
}
