/*
 * Copyright 2014-2019 Real Logic Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.aeron.cluster;

import io.aeron.ChannelUri;
import io.aeron.ExclusivePublication;
import io.aeron.FragmentAssembler;
import io.aeron.Image;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.archive.Archive;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.RecordingDescriptorConsumer;
import io.aeron.cluster.codecs.CloseReason;
import io.aeron.cluster.service.ClientSession;
import io.aeron.cluster.service.Cluster;
import io.aeron.cluster.service.ClusteredServiceContainer;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.status.SystemCounterDescriptor;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.collections.MutableLong;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.collections.MutableInteger;
import org.agrona.concurrent.status.CountersReader;

import java.io.File;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.aeron.Aeron.NULL_VALUE;
import static org.agrona.BitUtil.SIZE_OF_INT;

class TestNode implements AutoCloseable
{
    final ClusteredMediaDriver clusteredMediaDriver;
    private final ClusteredServiceContainer container;
    private final TestService service;
    private final TestNodeContext context;
    private boolean isClosed = false;

    TestNode(final TestNodeContext context)
    {
        clusteredMediaDriver = ClusteredMediaDriver.launch(
            context.mediaDriverContext,
            context.archiveContext,
            context.consensusModuleContext
                .terminationHook(TestUtil.dynamicTerminationHook(
                    context.terminationExpected, context.memberWasTerminated)));

        container = ClusteredServiceContainer.launch(
            context.serviceContainerContext
                .terminationHook(TestUtil.dynamicTerminationHook(
                    context.terminationExpected, context.serviceWasTerminated)));

        service = context.service;
        this.context = context;
    }

    ConsensusModule consensusModule()
    {
        return clusteredMediaDriver.consensusModule();
    }

    TestService service()
    {
        return service;
    }

    TestNodeContext testNodeContext() {
        return context;
    }

    public void close()
    {
        if (!isClosed)
        {
            CloseHelper.close(clusteredMediaDriver);
            CloseHelper.close(container);

            if (null != clusteredMediaDriver)
            {
                //clusteredMediaDriver.mediaDriver().context().deleteAeronDirectory();
            }

            isClosed = true;
        }
    }

    void cleanUp()
    {
        if (null != clusteredMediaDriver)
        {
//            clusteredMediaDriver.consensusModule().context().deleteDirectory();
//            clusteredMediaDriver.archive().context().deleteArchiveDirectory();
        }

        if (null != container)
        {
//            container.context().deleteDirectory();
        }
    }

    Cluster.Role role()
    {
        return Cluster.Role.get((int)clusteredMediaDriver.consensusModule().context().clusterNodeCounter().get());
    }

    boolean isClosed()
    {
        return isClosed;
    }

    Election.State electionState()
    {
        final MutableInteger electionStateValue = new MutableInteger(NULL_VALUE);

        countersReader().forEach(
            (counterId, typeId, keyBuffer, label) ->
            {
                if (typeId == Election.ELECTION_STATE_TYPE_ID)
                {
                    electionStateValue.value = (int)countersReader().getCounterValue(counterId);
                }
            });

        return NULL_VALUE != electionStateValue.value ? Election.State.get(electionStateValue.value) : null;
    }

    boolean isLeader()
    {
        return role() == Cluster.Role.LEADER;
    }

    boolean isFollower()
    {
        return role() == Cluster.Role.FOLLOWER;
    }

    void terminationExpected(final boolean terminationExpected)
    {
        context.terminationExpected.lazySet(terminationExpected);
    }

    boolean hasServiceTerminated()
    {
        return context.serviceWasTerminated.get();
    }

    boolean hasMemberTerminated()
    {
        return context.memberWasTerminated.get();
    }

    int index()
    {
        return service.index();
    }

    CountersReader countersReader()
    {
        return clusteredMediaDriver.mediaDriver().context().countersManager();
    }

    long errors()
    {
        return countersReader().getCounterValue(SystemCounterDescriptor.ERRORS.id());
    }

    ClusterTool.ClusterMembersInfo clusterMembersInfo()
    {
        final ClusterTool.ClusterMembersInfo clusterMembersInfo = new ClusterTool.ClusterMembersInfo();
        final File clusterDir = clusteredMediaDriver.consensusModule().context().clusterDir();

        if (!ClusterTool.listMembers(clusterMembersInfo, clusterDir, TimeUnit.SECONDS.toMillis(1)))
        {
            throw new IllegalStateException("timeout waiting for cluster members info");
        }

        return clusterMembersInfo;
    }

    void removeMember(final int followerMemberId, final boolean isPassive)
    {
        final File clusterDir = clusteredMediaDriver.consensusModule().context().clusterDir();

        if (!ClusterTool.removeMember(clusterDir, followerMemberId, isPassive))
        {
            throw new IllegalStateException("could not remove member");
        }
    }

    static class TestService extends StubClusteredService
    {
        private volatile int messageCount;
        private volatile boolean wasSnapshotTaken = false;
        private volatile boolean wasSnapshotLoaded = false;
        private final int index;
        private Cluster.Role role = Cluster.Role.FOLLOWER;

        private ExclusivePublication archivePublication;

        TestService(final int index)
        {
            this.index = index;
        }

        int index()
        {
            return index;
        }

        int messageCount()
        {
            return messageCount;
        }

        boolean wasSnapshotTaken()
        {
            return wasSnapshotTaken;
        }

        boolean wasSnapshotLoaded()
        {
            return wasSnapshotLoaded;
        }

        public void onStart(final Cluster cluster) {
            super.onStart(cluster);

            if (index == 2 && archivePublication == null) {
                AeronArchive aeronArchive = AeronArchive.connect(
                  new AeronArchive.Context()
                    .controlRequestChannel("aeron:udp?term-length=64k|endpoint=localhost:8012")
                    .controlRequestStreamId(100)
                    .controlResponseChannel("aeron:udp?endpoint=localhost:8099") // otherwise Address already in use aeron:udp?endpoint=localhost:8020
                    .ownsAeronClient(false)
                    .aeron(cluster.aeron()));

                int archiveStreamId = 9000;
                archivePublication = aeronArchive.addRecordedExclusivePublication("aeron:ipc", archiveStreamId);

                // Starting a Thread that will replay the archived messages. This could correspond to
                // a message consumer that starts from an offset and then consumes at its own pace,
                // including the live tail
                long fromPosition = 0L;
                TestServiceArchiveReplayer archiveReplayer = new TestServiceArchiveReplayer(aeronArchive, archivePublication.sessionId(), archiveStreamId, fromPosition);
                archiveReplayer.start();
            }
        }

        public void onSessionOpen(final ClientSession session, final long timestampMs)
        {
            System.out.println("onSessionOpen " + session.id());
        }

        public void onSessionClose(final ClientSession session, final long timestampMs, final CloseReason closeReason)
        {
            System.out.println("onSessionClose " + session.id() + " reason " + closeReason);
        }

        @Override
        public void onRoleChange(Cluster.Role newRole) {
            System.out.println("onRoleChange index " + index + " => " + newRole);
            role = newRole;
        }

        public void onSessionMessage(
            final ClientSession session,
            final long timestampMs,
            final DirectBuffer buffer,
            final int offset,
            final int length,
            final Header header)
        {

            int value = buffer.getInt(offset);
            System.out.println("onSessionMessage index " + index + " value " + value + " messageCount " + (messageCount + 1));

            if (archivePublication != null) {
                long archiveResult = archivePublication.offer(buffer, offset, length);
                while (archiveResult < 0) {
                    cluster.idle();
                    archiveResult = archivePublication.offer(buffer, offset, length);
                }
            }

            long offerResult = session.offer(buffer, offset, length);
            while (offerResult < 0)
            {
                cluster.idle();
                offerResult = session.offer(buffer, offset, length);
            }

            String mocked = (offerResult == ClientSession.MOCKED_OFFER) ? "mocked (not leader) " : "";
            System.out.println(mocked + "egress offer index " + index + " value " + value + " messageCount " + (messageCount + 1));

            ++messageCount;
        }

        public void onTakeSnapshot(final Publication snapshotPublication)
        {
            final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer();

            int length = 0;
            buffer.putInt(length, messageCount);
            length += SIZE_OF_INT;

            snapshotPublication.offer(buffer, 0, length);
            wasSnapshotTaken = true;
        }

        public void onLoadSnapshot(final Image snapshotImage)
        {
            final FragmentHandler handler = (buffer, offset, length, header) -> messageCount = buffer.getInt(offset);

            while (true)
            {
                final int fragments = snapshotImage.poll(handler, 1);

                if (fragments == 1)
                {
                    break;
                }

                cluster.idle();
            }

            wasSnapshotLoaded = true;
        }
    }

    static class TestServiceArchiveReplayer extends Thread {

        private static final int FRAGMENT_COUNT_LIMIT = 20;

        private final FragmentHandler fragmentHandler = new FragmentAssembler(this::onMessage);

        private final AeronArchive aeronArchive;
        private final int publicationSessionId;
        private final int archiveStreamId;
        private long fromPosition;

        private long messageCount = 0;

        TestServiceArchiveReplayer(AeronArchive aeronArchive, int publicationSessionId, int archiveStreamId, long fromPosition) {
            this.aeronArchive = aeronArchive;
            this.publicationSessionId = publicationSessionId;
            this.archiveStreamId = archiveStreamId;
            this.fromPosition = fromPosition;
        }


        @Override
        public void run() {

            long recordingId = findRecordingId(ChannelUri.addSessionId("aeron:ipc", publicationSessionId), archiveStreamId);
            System.out.println("starting replay of recordingId " + recordingId);

            try (Subscription subscription = aeronArchive.replay(
              recordingId, fromPosition, Long.MAX_VALUE, "aeron:ipc", archiveStreamId))
            {
                while (!subscription.isConnected())
                {
                    Thread.yield();
                }

                final IdleStrategy idleStrategy = new BackoffIdleStrategy(10, 10, 1000, 1000);

                while (true) {
                    final int fragments = subscription.poll(fragmentHandler, FRAGMENT_COUNT_LIMIT);
                    if (0 == fragments) {
                        if (!subscription.isConnected()) {
                            System.out.println("unexpected end of stream at message count: " + messageCount);
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
            System.out.println("archive replayed message value " + value + " messageCount " + (messageCount + 1));

            messageCount++;

            // simulate very slow consumer
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        private long findRecordingId(final String channel, int stream)
        {
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

            if (1 != recordingsFound)
            {
                throw new IllegalStateException("should have been one recording");
            }

            return foundRecordingId.get();
        }

    }

    static class TestNodeContext
    {
        public final MediaDriver.Context mediaDriverContext = new MediaDriver.Context();
        public final Archive.Context archiveContext = new Archive.Context();
        public final AeronArchive.Context aeronArchiveContext = new AeronArchive.Context();
        public final ConsensusModule.Context consensusModuleContext = new ConsensusModule.Context();
        public final ClusteredServiceContainer.Context serviceContainerContext =
          new ClusteredServiceContainer.Context();
        public final AtomicBoolean terminationExpected = new AtomicBoolean(false);
        public final AtomicBoolean memberWasTerminated = new AtomicBoolean(false);
        public final AtomicBoolean serviceWasTerminated = new AtomicBoolean(false);
        public TestService service = null;
    }
}
