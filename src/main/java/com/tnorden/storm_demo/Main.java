package com.tnorden.storm_demo;

import backtype.storm.LocalCluster;

/**
 * Created with IntelliJ IDEA.
 * User: tnorden@visiblemeasures.com
 * Date: 1/7/13
 * Time: 11:07 AM
 */
public class Main {
  public static void main(String[] args) {
    System.out.println("storm-demo");
    LocalCluster localCluster = new LocalCluster();
  }
}
