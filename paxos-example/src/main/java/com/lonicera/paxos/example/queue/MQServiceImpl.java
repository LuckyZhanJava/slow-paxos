package com.lonicera.paxos.example.queue;

import com.baidu.brpc.client.BrpcProxy;
import com.baidu.brpc.client.RpcClient;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.lonicera.paxos.core.errors.NotLeaderException;
import com.lonicera.paxos.core.protocol.Proposer;
import com.lonicera.paxos.core.protocol.Proposer.State;
import java.util.Map;
import lombok.extern.log4j.Log4j2;

@Log4j2
public class MQServiceImpl implements MQService {

  private Proposer proposer;
  private Map<Integer, String> proposerUriConfig;
  private static final ObjectMapper objectMapper = new ObjectMapper();

  public MQServiceImpl(Proposer proposer) {
    this.proposer = proposer;
    this.proposerUriConfig = proposer.getProposerUriConfig();
  }

  @Override
  public PutResult queue(Message message) throws Exception {
    String json = objectMapper.writeValueAsString(message);
    byte[] bytes = (json + "\r\n").getBytes("utf-8");
    State state = proposer.getState();
    if (state == State.STATE_CANDIDATE) {
      throw new IllegalStateException("Node is not leader");
    }
    if (state == State.STATE_FOLLOWER) {
      int leaderId = proposer.getLeaderId();
      RpcClient rpcClient = new RpcClient("list://" + proposerUriConfig.get(leaderId));
      MQService leaderService = BrpcProxy.getProxy(rpcClient, MQService.class);
      return leaderService.queue(message);
    }
    try {
      long start = System.nanoTime();
      long index = proposer.propose(bytes);
      long end = System.nanoTime();
      log.info("message queue cost : {}", end - start);
      return new PutResult(index);
    } catch (NotLeaderException e) {
      return queue(message);
    }
  }
}
