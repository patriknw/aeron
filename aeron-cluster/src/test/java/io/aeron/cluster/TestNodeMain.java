package io.aeron.cluster;

import io.aeron.Aeron;

public class TestNodeMain {
  public static void main(String[] args) throws Exception {
    int index = Integer.valueOf(args[0]);
    startNode(index);
  }

  private static void startNode(int index) throws InterruptedException {
    try (TestCluster testCluster = new TestCluster(3, 0, 2)) {
      TestNode node = testCluster.startStaticNode(index, false);

      Aeron aeron = node.consensusModule().context().aeron();
      System.out.println("consensusModule aeronDirectory " + aeron.context().aeronDirectory());

      Thread.sleep(60000);
    }
  }

}
