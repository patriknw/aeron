package io.aeron.cluster.pubsub;

import io.aeron.CommonContext;
import io.aeron.ExclusivePublication;
import io.aeron.archive.client.AeronArchive;
import io.aeron.cluster.ClusteredMediaDriver;
import io.aeron.cluster.service.ClusteredServiceContainer;
import org.agrona.CloseHelper;
import org.agrona.concurrent.SigInt;

import java.io.File;

import static io.aeron.cluster.pubsub.MediaDriverLauncher.memberSpecificPort;

public class ClusterNodeMain implements AutoCloseable {

    private static final String ARCHIVE_CONTROL_REQUEST_CHANNEL =
      "aeron:udp?term-length=64k|endpoint=localhost:8010";
    private static final String ARCHIVE_CONTROL_RESPONSE_CHANNEL =
      "aeron:udp?term-length=64k|endpoint=localhost:8020";

    private final ClusteredMediaDriver mediaDriver;
    private final ClusteredServiceContainer serviceContainer;
    private final AeronArchive aeronArchive;
    private final ExclusivePublication archivePublication;
    private final int archiveStreamId = 900;

    private final int nodeIndex;
    private final String aeronDir;

    public static void main(String[] args) {
        int nodeIndex = Integer.valueOf(args[0]);

        final String aeronDir = CommonContext.getAeronDirectoryName() + "-cluster-node-" + nodeIndex;
        ClusterNodeMain node = new ClusterNodeMain(nodeIndex, aeronDir);

        SigInt.register(node::close);

        node.startConsumer("consumer-1", 0);
        node.startConsumer("consumer-2", 0);
    }

    public ClusterNodeMain(int nodeIndex, String aeronDir) {
        this.nodeIndex = nodeIndex;
        this.aeronDir = aeronDir;
        System.out.println("Cluster node " + nodeIndex + " aeron dir: " + aeronDir);

        int appointedLeaderId = -1; // -1 for leader election
        int memberCount = 3;
        String aeronDriverDir = aeronDir + "-driver";

        AeronArchive.Context aeronArchiveContext = new AeronArchive.Context();
        aeronArchiveContext
          .controlRequestChannel(memberSpecificPort(ARCHIVE_CONTROL_REQUEST_CHANNEL, nodeIndex))
          .controlRequestStreamId(100)
          .controlResponseChannel(memberSpecificPort(ARCHIVE_CONTROL_RESPONSE_CHANNEL, nodeIndex))
          .controlResponseStreamId(110 + nodeIndex)
          .aeronDirectoryName(aeronDir);

        mediaDriver = MediaDriverLauncher.launchClusteredMediaDriver(nodeIndex, appointedLeaderId, memberCount,
          aeronArchiveContext, aeronDir, aeronDriverDir, false);

        aeronArchive = AeronArchive.connect(
          new AeronArchive.Context()
            .controlRequestChannel(aeronArchiveContext.controlRequestChannel())
            .controlRequestStreamId(100)
            .controlResponseChannel("aeron:udp?endpoint=localhost:901" + nodeIndex ) // unique port otherwise Address already in use
            .aeronDirectoryName(aeronDriverDir));
        archivePublication = aeronArchive.addRecordedExclusivePublication("aeron:ipc", archiveStreamId);

        ClusteredServiceContainer.Context serviceContainerContext = new ClusteredServiceContainer.Context();

        Topic topicService = new Topic(archivePublication);

        serviceContainerContext
          .aeronDirectoryName(aeronDriverDir)
          .archiveContext(aeronArchiveContext.clone())
          .clusterDir(new File(aeronDir, "topicService"))
          .clusteredService(topicService)
          .errorHandler(Throwable::printStackTrace);

        serviceContainer = ClusteredServiceContainer.launch(serviceContainerContext);
    }

    public void startConsumer(String consumerName, int slowRndMs) {
        // Starting a Thread that will replay the archived messages. This could correspond to
        // a message consumer that starts from an offset and then consumes at its own pace,
        // including the live tail
        long fromPosition = 0L;
        Consumer consumer = new Consumer(aeronArchive, archivePublication.sessionId(), archiveStreamId, fromPosition,
          consumerName, slowRndMs);
        consumer.start();
    }

    @Override // AutoClosable
    public void close() {
        CloseHelper.close(serviceContainer);
        CloseHelper.close(archivePublication);
        CloseHelper.close(aeronArchive);
        CloseHelper.close(mediaDriver);
    }
}
