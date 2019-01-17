package io.aeron.cluster.pubsub;

import io.aeron.CommonContext;
import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.client.EgressListener;
import io.aeron.cluster.codecs.EventCode;
import io.aeron.driver.MediaDriver;
import io.aeron.logbuffer.Header;
import org.agrona.BitUtil;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.collections.MutableInteger;
import org.agrona.concurrent.SigInt;

public class PublisherMain implements AutoCloseable {

    private final String aeronDir;
    private final MediaDriver mediaDriver;
    private final AeronCluster client;

    private final ExpandableArrayBuffer msgBuffer = new ExpandableArrayBuffer();

    private final MutableInteger responseAck = new MutableInteger(-1);
    private final EgressListener egressMessageListener = new EgressListener() {
        @Override
        public void onMessage(long clusterSessionId, long timestampMs, DirectBuffer buffer, int offset, int length,
                              Header header) {
            int value = buffer.getInt(offset);
            // System.out.println("egressMessageListener received ack for value " + value);
            responseAck.set(value);
        }

        @Override
        public void newLeader(long clusterSessionId, long leadershipTermId, int leaderMemberId,
                              String memberEndpoints) {
            System.out.println("egressMessageListener newLeader " + leaderMemberId);
        }

        @Override
        public void sessionEvent(long correlationId, long clusterSessionId, long leadershipTermId,
                                 int leaderMemberId, EventCode code, String detail) {
            System.out.println("egressMessageListener sessionEvent " + code + " " + detail);
        }
    };

    public static void main(String[] args) {
        final String aeronDir = CommonContext.getAeronDirectoryName() + "-publisher";

        try (PublisherMain publisherMain = new PublisherMain(aeronDir)) {
            SigInt.register(publisherMain::close);

            publisherMain.publishSingleMessage(1);
            publisherMain.awaitResponseAck(1);

            publisherMain.publishManyMessagesOneByOne(100);
            publisherMain.publishManyMessagesOneByOne(100);
            publisherMain.publishManyMessagesOneByOne(100);

            publisherMain.publishManyMessagesInBatch(100);
            publisherMain.publishManyMessagesInBatch(100);
            publisherMain.publishManyMessagesInBatch(100);
        }


    }

    public PublisherMain(String aeronDir) {
        this.aeronDir = aeronDir;
        System.out.println("Publisher aeron dir: " + aeronDir);

        mediaDriver = MediaDriverLauncher.launchClientMediaDriver(aeronDir);

        int memberCount = 3;
        client = AeronCluster.connect(
          new AeronCluster.Context()
            .egressListener(egressMessageListener)
            .aeronDirectoryName(aeronDir)
            .ingressChannel("aeron:udp")
            .clusterMemberEndpoints(clientMemberEndpoints(memberCount)));
    }

    private static String clientMemberEndpoints(final int memberCount) {
        final StringBuilder builder = new StringBuilder();

        for (int i = 0; i < memberCount; i++) {
            builder
              .append(i).append('=')
              .append("localhost:2011").append(i).append(',');
        }

        builder.setLength(builder.length() - 1);

        return builder.toString();
    }

    public void publishSingleMessage(int value) {
        msgBuffer.putInt(0, value);
        while (client.offer(msgBuffer, 0, BitUtil.SIZE_OF_INT) < 0) {
            checkInterrupted();
            client.pollEgress();
            Thread.yield();
        }

        client.pollEgress();
    }

    public void awaitResponseAck(int value) {
        while (responseAck.get() != value) {
            checkInterrupted();
            Thread.yield();
            client.pollEgress();
        }
    }

    public void publishManyMessagesOneByOne(int count) {
        long startTime = System.nanoTime();
        for (int i = 0; i < count; i++) {
            publishSingleMessage(i);
            awaitResponseAck(i);
        }
        printDuration("one-by-one", count, startTime);
    }

    public void publishManyMessagesInBatch(int count) {
        long startTime = System.nanoTime();
        for (int i = 0; i < count; i++) {
            publishSingleMessage(i);
        }
        awaitResponseAck(count -1);
        printDuration("batch", count, startTime);
    }

    private void printDuration(String prefix, int count, long startTime) {
        long durationMicros = (System.nanoTime() - startTime) / 1000;
        String duration;
        if (durationMicros < 5000)
            duration = "" + durationMicros + " µs";
        else
            duration = "" + (durationMicros / 1000) + " ms";
        System.out.println(prefix + ": Publishing " + count + " messages took " + duration);
    }

    private void checkInterrupted() {
        if (Thread.currentThread().isInterrupted())
            throw new RuntimeException("interrupted");
    }

    @Override // AutoClosable
    public void close() {
        CloseHelper.close(client);
        CloseHelper.close(mediaDriver);
    }
}
