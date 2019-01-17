package io.aeron.cluster.pubsub;

import io.aeron.cluster.client.AeronCluster;

public class Publisher {
    private final AeronCluster client;

    public Publisher(AeronCluster client) {
        this.client = client;
    }
}
