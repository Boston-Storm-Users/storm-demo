package com.tnorden.storm_demo.trident.functions;

import storm.trident.operation.BaseFilter;
import storm.trident.tuple.TridentTuple;

/**
 * Created with IntelliJ IDEA.
 * User: tnorden@visiblemeasures.com
 * Date: 1/14/13
 * Time: 11:03 AM
 */
public class UserAgentFilter extends BaseFilter {
  public boolean isKeep(TridentTuple tuple) {
    return tuple.getStringByField("user-agent").contains(tuple.getStringByField("args"));
  }
}
