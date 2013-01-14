package com.tnorden.storm_demo.trident.spout;

import backtype.storm.Config;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.mortbay.jetty.HttpStatus;
import storm.trident.operation.TridentCollector;
import storm.trident.spout.IBatchSpout;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.*;

/**
 * Created with IntelliJ IDEA.
 * User: tnorden@visiblemeasures.com
 * Date: 1/11/13
 * Time: 11:18 AM
 */
public class HttpSpout implements IBatchSpout {
  private HttpServer httpServer;
  private int batchSize, port;

  private static Queue<HttpExchange> exchangeQueue = new ConcurrentLinkedQueue<HttpExchange>();
  private static Map<Long, List<HttpExchange>> batches = new ConcurrentHashMap<Long, List<HttpExchange>>();

  public HttpSpout(int batchSize, int port) {
    this.batchSize = batchSize;
    this.port = port;
  }

  public void startServer() {
    if (httpServer == null) {
      try {
        httpServer = HttpServer.create(new InetSocketAddress(8088), 100);
        httpServer.createContext("/", new MyHandler());
        httpServer.setExecutor(new ThreadPoolExecutor(15, 30, 60, TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(100)));
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
    httpServer.start();
  }

  private void stopServer() {
    httpServer.stop(2);
  }

  static class MyHandler implements HttpHandler {

    @Override
    public void handle(HttpExchange httpExchange) throws IOException {
      if(exchangeQueue.offer(httpExchange)) {
        try {
          synchronized (httpExchange) {
            httpExchange.wait();
          }
        } catch (InterruptedException e) { }
      } else {
        respond(httpExchange, HttpStatus.ORDINAL_500_Internal_Server_Error, "spout queue is full", false);
      }

    }
  }

  static void respond(HttpExchange httpExchange, int statusCode, String message, boolean blocking) {
    try {
      httpExchange.sendResponseHeaders(statusCode, message.length());
      OutputStream outputStream = httpExchange.getResponseBody();
      outputStream.write(message.getBytes());
      outputStream.close();
      if (blocking) {
        synchronized (httpExchange) {
          httpExchange.notify();
        }
      }
    } catch (IOException e) { }
  }

  @Override
  public void open(Map map, TopologyContext topologyContext) {
    startServer();
  }

  @Override
  public void emitBatch(long l, TridentCollector tridentCollector) {
    List<HttpExchange> batch = new ArrayList<HttpExchange>();
    for (int i = 0; i < batchSize; i++) {
      HttpExchange httpExchange = exchangeQueue.poll();
      if (httpExchange != null) {
        batch.add(httpExchange);
        tridentCollector.emit(new Values(System.currentTimeMillis(),
            httpExchange.getRemoteAddress().getHostName(),
            httpExchange.getRequestURI(),
            httpExchange.getRequestHeaders().getFirst("User-Agent")));
      } else {
        break;
      }
    }
    if (!batch.isEmpty()) {
      batches.put(l, batch);
    }
  }

  @Override
  public void ack(long l) {
    List<HttpExchange> batch = batches.get(l);
    if (batch != null) {
      for (HttpExchange httpExchange : batch) {
        respond(httpExchange, HttpStatus.ORDINAL_200_OK, "ack", true);
      }
      batches.remove(l);
    }
  }
  @Override
  public void close() {
    dumpCache(HttpStatus.ORDINAL_503_Service_Unavailable, "server is shutting down");
    stopServer();
  }

  public void dumpCache(int statusCode, String message) {
    while (!exchangeQueue.isEmpty()) {
      HttpExchange httpExchange = exchangeQueue.poll();
      respond(httpExchange, statusCode, message, true);
    }
    for (Map.Entry<Long, List<HttpExchange>> batch : batches.entrySet()) {
      for (HttpExchange httpExchange : batch.getValue()) {
        respond(httpExchange, statusCode, message, true);
      }
    }
  }

  @Override
  public Map getComponentConfiguration() {
    Config conf = new Config();
    conf.setMaxTaskParallelism(1);
    return conf;
  }

  @Override
  public Fields getOutputFields() {
    return new Fields("timestamp", "remoteHost", "uri", "user-agent");
  }
}
