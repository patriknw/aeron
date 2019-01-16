package io.aeron.cluster;

public class RestartLeaderScenarioMain {
  public static void main(String[] args) throws Exception {
    restartLeaderScenario();
  }

  private static void restartLeaderScenario() throws InterruptedException {
    try (TestCluster testCluster = new TestCluster(3, 0, -1)) {
      TestNode node0 = testCluster.startStaticNode(0, true);
      TestNode node1 = testCluster.startStaticNode(1, true);
      TestNode node2 = testCluster.startStaticNode(2, true);

      TestNode leader = testCluster.awaitLeader();
      System.out.println("leader " + leader.index());

      testCluster.startClient(0);
      testCluster.sendMessages(3, 1);
      testCluster.awaitResponses(3);

      leader.close();
      System.out.println("leader stopped");

      TestNode newLeader = testCluster.awaitLeader(leader.index());
      System.out.println("new leader " + leader.index());

      testCluster.sendMessages(1, 4);
      testCluster.sendMessages(1, 5);
      testCluster.awaitResponses(5);

      Thread.sleep(3000);
      System.out.println("old leader node starting again");
      TestNode oldLeaderAgain = testCluster.startStaticNode(leader.index(), true);
      testCluster.awaitMessageCountForService(oldLeaderAgain, 5);
    }
  }

}
