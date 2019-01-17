package io.aeron.cluster.pubsub;

import io.aeron.archive.Archive;
import io.aeron.archive.ArchiveThreadingMode;
import io.aeron.archive.client.AeronArchive;
import io.aeron.cluster.ClusteredMediaDriver;
import io.aeron.cluster.ConsensusModule;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.MinMulticastFlowControlSupplier;
import io.aeron.driver.ThreadingMode;

import java.io.File;

public class MediaDriverLauncher {

    private static final long MAX_CATALOG_ENTRIES = 1024;
    private static final String LOG_CHANNEL =
      "aeron:udp?term-length=256k|control-mode=manual|control=localhost:55550";

    public static MediaDriver launchClientMediaDriver(String aeronDir) {
        return MediaDriver.launch(
          new MediaDriver.Context()
            .threadingMode(ThreadingMode.SHARED)
            .aeronDirectoryName(aeronDir));
    }

    public static ClusteredMediaDriver launchClusteredMediaDriver(
      int nodeIndex, int appointedLeaderId, int memberCount,
      AeronArchive.Context aeronArchiveContext, String aeronDir, String aeronDriverDir,
      boolean cleanStart) {

        MediaDriver.Context mediaDriverContext = new MediaDriver.Context();
        Archive.Context archiveContext = new Archive.Context();
        ConsensusModule.Context consensusModuleContext = new ConsensusModule.Context();

        mediaDriverContext
          .aeronDirectoryName(aeronDriverDir)
          .threadingMode(ThreadingMode.SHARED)
          .termBufferSparseFile(true)
          .multicastFlowControlSupplier(new MinMulticastFlowControlSupplier())
          .errorHandler(Throwable::printStackTrace)
          .dirDeleteOnStart(true);

        archiveContext
          .maxCatalogEntries(MAX_CATALOG_ENTRIES)
          .aeronDirectoryName(aeronDriverDir)
          .archiveDir(new File(aeronDir, "archive"))
          .controlChannel(aeronArchiveContext.controlRequestChannel())
          .controlStreamId(aeronArchiveContext.controlRequestStreamId())
          .localControlChannel("aeron:ipc?term-length=64k")
          .localControlStreamId(aeronArchiveContext.controlRequestStreamId())
          .threadingMode(ArchiveThreadingMode.SHARED)
          .deleteArchiveOnStart(cleanStart);


        consensusModuleContext
          .errorHandler(Throwable::printStackTrace)
          .clusterMemberId(nodeIndex)
          .clusterMembers(clusterMembersString(memberCount))
          .appointedLeaderId(appointedLeaderId)
          .aeronDirectoryName(aeronDriverDir)
          .clusterDir(new File(aeronDir, "consensus-module"))
          .ingressChannel("aeron:udp?term-length=64k")
          .logChannel(memberSpecificPort(LOG_CHANNEL, nodeIndex))
          .archiveContext(aeronArchiveContext.clone())
          .deleteDirOnStart(cleanStart);

        return ClusteredMediaDriver.launch(
          mediaDriverContext,
          archiveContext,
          consensusModuleContext);
    }

    public static String memberSpecificPort(final String channel, final int nodeIndex) {
        return channel.substring(0, channel.length() - 1) + nodeIndex;
    }

    private static String clusterMembersString(final int memberCount) {
        final StringBuilder builder = new StringBuilder();

        for (int i = 0; i < memberCount; i++) {
            builder
              .append(i).append(',')
              .append("localhost:2011").append(i).append(',')
              .append("localhost:2022").append(i).append(',')
              .append("localhost:2033").append(i).append(',')
              .append("localhost:2044").append(i).append(',')
              .append("localhost:801").append(i).append('|');
        }

        builder.setLength(builder.length() - 1);

        return builder.toString();
    }
}
