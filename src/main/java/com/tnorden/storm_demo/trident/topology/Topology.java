package com.tnorden.storm_demo.trident.topology;

import backtype.storm.Config;
import backtype.storm.generated.StormTopology;

/**
 * Created with IntelliJ IDEA.
 * User: tnorden@visiblemeasures.com
 * Date: 1/7/13
 * Time: 4:19 PM
 */
public interface Topology {
  public String topologyName();
  public StormTopology build();
  public Config getConfig();
}
