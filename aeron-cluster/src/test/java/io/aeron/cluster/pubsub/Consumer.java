package io.aeron.cluster.pubsub;

import io.aeron.ChannelUri;
import io.aeron.FragmentAssembler;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.RecordingDescriptorConsumer;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;
import org.agrona.collections.MutableLong;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.IdleStrategy;

import java.util.concurrent.ThreadLocalRandom;

public class Consumer extends Thread {
    private static final int FRAGMENT_COUNT_LIMIT = 20;

    private final FragmentHandler fragmentHandler = new FragmentAssembler(this::onMessage);

    private final AeronArchive aeronArchive;
    private final int publicationSessionId;
    private final int archiveStreamId;
    private long fromPosition;
    private final String consumerName;
    private final int slowRndMs;

    private long messageCount = 0;

    public Consumer(AeronArchive aeronArchive, int publicationSessionId, int archiveStreamId, long fromPosition,
                    String consumerName, int slowRndMs) {
        this.aeronArchive = aeronArchive;
        this.publicationSessionId = publicationSessionId;
        this.archiveStreamId = archiveStreamId;
        this.fromPosition = fromPosition;
        this.consumerName = consumerName;
        this.slowRndMs = slowRndMs;
    }


    @Override
    public void run() {

        long recordingId = findRecordingId(ChannelUri.addSessionId("aeron:ipc", publicationSessionId), archiveStreamId);
        System.out.println(consumerName + " starting replay of recordingId " + recordingId);

        try (Subscription subscription = aeronArchive.replay(
          recordingId, fromPosition, Long.MAX_VALUE, "aeron:ipc", archiveStreamId)) {
            while (!subscription.isConnected()) {
                Thread.yield();
            }

            final IdleStrategy idleStrategy = new BackoffIdleStrategy(10, 10, 1000, 1000);

            while (true) {
                final int fragments = subscription.poll(fragmentHandler, FRAGMENT_COUNT_LIMIT);
                if (0 == fragments) {
                    if (!subscription.isConnected()) {
                        System.out.println(consumerName + " unexpected end of stream at message count: " + messageCount);
                        return;
                    }
                }

                if (Thread.currentThread().isInterrupted())
                    throw new RuntimeException("TestServiceArchiveSubscriber");

                idleStrategy.idle(fragments);
            }
        }
    }

    private void onMessage(final DirectBuffer buffer, final int offset, final int length, final Header header) {
        int value = buffer.getInt(offset);
        System.out.println(consumerName + " replayed message value " + value + " messageCount " + (messageCount + 1));

        messageCount++;

        if (slowRndMs > 0) {
            // simulate slow consumer
            try {
                Thread.sleep(ThreadLocalRandom.current().nextInt(slowRndMs));
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private long findRecordingId(final String channel, int stream) {
        final MutableLong foundRecordingId = new MutableLong();

        final RecordingDescriptorConsumer consumer =
          (controlSessionId,
           correlationId,
           recordingId,
           startTimestamp,
           stopTimestamp,
           startPosition,
           stopPosition,
           initialTermId,
           segmentFileLength,
           termBufferLength,
           mtuLength,
           sessionId,
           streamId,
           strippedChannel,
           originalChannel,
           sourceIdentity) -> foundRecordingId.set(recordingId);

        final int recordingsFound = aeronArchive.listRecordingsForUri(
          0L, 10, channel, stream, consumer);

        if (1 != recordingsFound) {
            throw new IllegalStateException("should have been one recording");
        }

        return foundRecordingId.get();
    }
}
