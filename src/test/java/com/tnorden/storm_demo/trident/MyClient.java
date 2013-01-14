package com.tnorden.storm_demo.trident;

import backtype.storm.generated.DRPCExecutionException;
import backtype.storm.utils.DRPCClient;
import org.apache.thrift7.TException;

/**
 * Created with IntelliJ IDEA.
 * User: tnorden@visiblemeasures.com
 * Date: 1/14/13
 * Time: 8:38 AM
 */
public class MyClient {
  public static void main(String[] args) throws TException, DRPCExecutionException {
    DRPCClient drpcClient = new DRPCClient("localhost", 3772);
    System.out.println(drpcClient.execute("dump", ""));
    System.out.println(drpcClient.execute("path-requests", ""));
    System.out.println(drpcClient.execute("user-agents", ""));
    System.out.println(drpcClient.execute("filtered-user-agents", "Chrome"));
  }
}
