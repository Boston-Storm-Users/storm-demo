package com.tnorden.storm_demo.trident.topology;

import backtype.storm.LocalCluster;
import backtype.storm.utils.Utils;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Created with IntelliJ IDEA.
 * User: tnorden@visiblemeasures.com
 * Date: 1/14/13
 * Time: 11:54 AM
 */
public class TridentDemoTest {
  LocalCluster localCluster;

  @BeforeMethod
  public void setUp() throws Exception {
    localCluster = new LocalCluster();
  }

  @Test
  public void testTridentDemo() {
    TridentDemo tridentDemo = new TridentDemo();
    localCluster.submitTopology(tridentDemo.topologyName(),
        tridentDemo.getConfig(),
        tridentDemo.build());
    Utils.sleep(120000);
    localCluster.killTopology(tridentDemo.topologyName());
  }

  @AfterMethod
  public void tearDown() throws Exception {
    localCluster.shutdown();
  }
}
