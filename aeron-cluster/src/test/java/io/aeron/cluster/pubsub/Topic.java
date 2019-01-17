package io.aeron.cluster.pubsub;

import io.aeron.ExclusivePublication;
import io.aeron.Image;
import io.aeron.Publication;
import io.aeron.cluster.codecs.CloseReason;
import io.aeron.cluster.service.ClientSession;
import io.aeron.cluster.service.Cluster;
import io.aeron.cluster.service.ClusteredService;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;

public class Topic implements ClusteredService {
    private final ExclusivePublication archivePublication;

    private Cluster cluster;
    private Cluster.Role role = Cluster.Role.FOLLOWER;
    private int messageCount;

    public Topic(ExclusivePublication archivePublication) {
        this.archivePublication = archivePublication;
    }

    @Override
    public void onStart(Cluster cluster) {
        this.cluster = cluster;
        System.out.println("Started topic service");
    }

    @Override
    public void onSessionOpen(ClientSession session, long timestampMs) {
        System.out.println("onSessionOpen " + session.id());
    }

    @Override
    public void onSessionClose(ClientSession session, long timestampMs, CloseReason closeReason) {
        System.out.println("onSessionClose " + session.id() + " reason " + closeReason);
    }

    @Override
    public void onSessionMessage(ClientSession session, long timestampMs, DirectBuffer buffer, int offset,
                                 int length, Header header) {

        ++messageCount;

        int value = buffer.getInt(offset);
        System.out.println("onSessionMessage value " + value + " messageCount " + messageCount);

        if (archivePublication != null) {
            long archiveResult = archivePublication.offer(buffer, offset, length);
            while (archiveResult < 0) {
                if (archivePublication.isClosed()) {
                    System.out.println("archivePublication is closed");
                    break;
                }
                cluster.idle();
                archiveResult = archivePublication.offer(buffer, offset, length);
            }
        }

        long offerResult = session.offer(buffer, offset, length);
        while (offerResult < 0) {
            cluster.idle();
            offerResult = session.offer(buffer, offset, length);
        }

        String mocked = (offerResult == ClientSession.MOCKED_OFFER) ? "mocked (not leader) " : "";
        System.out.println(mocked + "egress offer value " + value + " messageCount " + messageCount);
    }

    @Override
    public void onTimerEvent(long correlationId, long timestampMs) {
    }

    @Override
    public void onTakeSnapshot(Publication snapshotPublication) {
    }

    @Override
    public void onLoadSnapshot(Image snapshotImage) {
    }

    @Override
    public void onRoleChange(Cluster.Role newRole) {
        System.out.println("onRoleChange " + role + " => " + newRole);
    }

    @Override
    public void onTerminate(Cluster cluster) {
    }
}
