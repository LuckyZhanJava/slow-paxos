package com.lonicera.paxos.example.queue;

import com.baidu.brpc.client.BrpcProxy;
import com.baidu.brpc.client.RpcClient;
import java.util.concurrent.ExecutionException;
import lombok.extern.log4j.Log4j2;

@Log4j2
public class PaxosClientApp {

  public static void main(String[] args) throws InterruptedException, ExecutionException {


    String serverUrls = "list://127.0.0.1:18111,127.0.0.1:18112,127.0.0.1:18113";

    RpcClient rpcClient = new RpcClient(serverUrls);

    MQService exampleService = BrpcProxy.getProxy(rpcClient, MQService.class);

    for (int i = 0; i < 10; i++) {
      Message message = new Message("topic", "value" + i);
      long start = System.nanoTime();
      try {
        exampleService.queue(message);
      } catch (Exception e) {
        log.error(e);
      }finally {
        long end = System.nanoTime();
        log.info("queue cost : {}", end - start);
      }
    }
  }
}
