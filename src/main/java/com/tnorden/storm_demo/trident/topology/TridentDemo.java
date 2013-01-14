package com.tnorden.storm_demo.trident.topology;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import com.google.common.collect.Lists;
import com.tnorden.storm_demo.trident.functions.UserAgentFilter;
import com.tnorden.storm_demo.trident.spout.HttpSpout;
import storm.trident.Stream;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.builtin.Count;
import storm.trident.operation.builtin.Sum;
import storm.trident.operation.builtin.TupleCollectionGet;
import storm.trident.testing.LRUMemoryMapState;

/**
 * Created with IntelliJ IDEA.
 * User: tnorden@visiblemeasures.com
 * Date: 1/7/13
 * Time: 3:51 PM
 */
public class TridentDemo implements Topology {

  @Override
  public StormTopology build() {
    TridentTopology tridentTopology = new TridentTopology();

    LRUMemoryMapState.Factory httpRequestMemoryMap = new LRUMemoryMapState.Factory(1000);

    HttpSpout httpSpout = new HttpSpout(10, 8088);
    Stream httpRequestStream = tridentTopology.newStream("httpRequests", httpSpout);

    TridentState httpRequestState = httpRequestStream
        .groupBy(httpSpout.getOutputFields())
        .persistentAggregate(httpRequestMemoryMap, new Count(), new Fields("count"));
    Fields stateFields = new Fields("timestamp", "remoteHost", "uri", "user-agent", "count");

    /*
    DRPC stream that returns all entries in the TridentState
     */
    tridentTopology.newDRPCStream("dump")
        .stateQuery(httpRequestState, new Fields("args"), new TupleCollectionGet(), stateFields);

    /*
    DRPC stream that returns count of requests for each path
     */
    tridentTopology.newDRPCStream("path-requests")
        .stateQuery(httpRequestState, new Fields("args"), new TupleCollectionGet(), stateFields)
        .groupBy(new Fields("uri"))
        .aggregate(new Fields("count"), new Sum(), new Fields("pathSum"))
        .project(new Fields("uri", "pathSum"));

    /*
    DRPC stream that returns count of requests by each user-agent
     */
    tridentTopology.newDRPCStream("user-agents")
        .stateQuery(httpRequestState, new Fields("args"), new TupleCollectionGet(), stateFields)
        .groupBy(new Fields("user-agent"))
        .aggregate(new Fields("count"), new Sum(), new Fields("user-agentSum"))
        .project(new Fields("user-agent", "user-agentSum"));

    /*
    DRPC stream that filters user-agents by the args passed and returns count of requests by each user-agent
     */
    tridentTopology.newDRPCStream("filtered-user-agents")
        .stateQuery(httpRequestState, new Fields("args"), new TupleCollectionGet(), stateFields)
        .each(new Fields("args", "user-agent"), new UserAgentFilter())
        .groupBy(new Fields("user-agent"))
        .aggregate(new Fields("count"), new Sum(), new Fields("user-agentSum"))
        .project(new Fields("user-agent", "user-agentSum"));

    return tridentTopology.build();
  }

  @Override
  public Config getConfig() {
    Config config = new Config();
    config.setDebug(false);
    config.put(Config.DRPC_SERVERS, Lists.newArrayList("localhost"));
    config.put(Config.STORM_CLUSTER_MODE, new String("distributed"));
    return config;
  }

  @Override
  public String topologyName() {
    return "tridentDemo";  //To change body of implemented methods use File | Settings | File Templates.
  }

  public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException {
    TridentDemo tridentDemo = new TridentDemo();

    StormSubmitter.submitTopology(tridentDemo.topologyName(), tridentDemo.getConfig(), tridentDemo.build());
  }
}
