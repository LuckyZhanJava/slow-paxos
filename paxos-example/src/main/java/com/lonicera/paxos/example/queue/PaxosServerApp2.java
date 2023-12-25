package com.lonicera.paxos.example.queue;

import com.baidu.brpc.server.RpcServer;
import com.lonicera.paxos.core.protocol.Proposer;
import com.lonicera.paxos.core.protocol.ProposerReader;
import com.lonicera.paxos.core.server.ProposerConfig;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import lombok.extern.log4j.Log4j2;

@Log4j2
public class PaxosServerApp2 {

  public static void main(String[] args) throws Exception {

    Map<Integer, String> proposerUriMap = new HashMap<Integer, String>() {
      {
        put(1, "127.0.0.1:18011");
        put(2, "127.0.0.1:18012");
        put(3, "127.0.0.1:18013");
      }
    };

    ProposerReader reader = new MessageQueueProposerReader();

    ProposerConfig config = new ProposerConfig(
        2,
        proposerUriMap,
        5000,
        5000,
        "e://paxos2"
    );

    Proposer proposer = new Proposer(
        config,
        reader
    );

    RpcServer server = new RpcServer(18112);

    try {
      proposer.start();
      MQService mqService = new MQServiceImpl(proposer);
      server.registerService(mqService);
      server.start();
    } catch (Exception e) {
      log.error("proposer start error", e);
      server.shutdown();
      proposer.shutdown();
    }

    Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          server.shutdown();
          proposer.shutdown();
        } catch (IOException e) {
          log.error(e);
        }
      }
    }));
  }
}
