package io.aeron.cluster;

public class TestClientMain {
  public static void main(String[] args) throws Exception {
      startClient(100);
  }

  private static void startClient(int index) throws InterruptedException {
    try (TestCluster testCluster = new TestCluster(3, 0, 2)) {

      testCluster.startClient(index);

      System.out.println("ingress channel: " +
        testCluster.client().ingressPublication().channel() + "/" + testCluster.client().ingressPublication().streamId());
      System.out.println("egress channel: " +
        testCluster.client().egressSubscription().channel() + "/" + testCluster.client().egressSubscription().streamId());

      testCluster.sendMessages(3, 1);
      testCluster.awaitResponses(3);

      Thread.sleep(3000);
    }
  }
}
