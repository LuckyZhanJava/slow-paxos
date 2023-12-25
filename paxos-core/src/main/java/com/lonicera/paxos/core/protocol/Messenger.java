package com.lonicera.paxos.core.protocol;

import com.lonicera.paxos.core.apis.BallotNumber;
import com.lonicera.paxos.core.apis.BallotVoteResponse;
import com.lonicera.paxos.core.apis.BeginBallotRequest;
import com.lonicera.paxos.core.apis.BeginElectRequest;
import com.lonicera.paxos.core.apis.DecreeReplicateRequest;
import com.lonicera.paxos.core.apis.DecreeReplicateResponse;
import com.lonicera.paxos.core.apis.ElectVoteResponse;
import com.lonicera.paxos.core.apis.ExtendLeaderTermRequest;
import com.lonicera.paxos.core.apis.ExtendLeaderTermResponse;
import com.lonicera.paxos.core.apis.GeneralDecree;
import com.lonicera.paxos.core.apis.LastElectResponse;
import com.lonicera.paxos.core.apis.LastVoteResponse;
import com.lonicera.paxos.core.apis.LeaderDecree;
import com.lonicera.paxos.core.apis.NextBallotRequest;
import com.lonicera.paxos.core.apis.NextElectRequest;
import com.lonicera.paxos.core.apis.PaxosApis;
import com.lonicera.paxos.core.apis.RecordDecreeRequest;
import com.lonicera.paxos.core.apis.RecordDecreeResponse;
import com.lonicera.paxos.core.apis.SuccessElectRequest;
import com.lonicera.paxos.core.apis.SuccessElectResponse;
import com.lonicera.paxos.core.rpc.ApiRequest;
import com.lonicera.paxos.core.rpc.ApiResponse;
import com.lonicera.paxos.core.rpc.RpcChannel;
import com.lonicera.paxos.core.rpc.RpcChannelFactory;
import com.lonicera.paxos.core.service.BallotService;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Messenger {

  private int proposerId;
  private Proposer proposer;
  private BallotService localService;
  private Map<Integer, RpcChannelFactory> proposerChannelFactoryMap;
  private int requestTimeoutMillis;

  public Messenger(
      Proposer proposer,
      Map<Integer, String> proposerConfigMap,
      int requestTimeoutMillis
  ) {
    this.proposerId = proposer.getId();
    this.proposer = proposer;
    this.localService = new BallotService(proposer);
    proposerChannelFactoryMap = new HashMap<>();
    for (Entry<Integer, String> entry : proposerConfigMap.entrySet()) {
      Integer id = entry.getKey();
      String uri = entry.getValue();
      proposerChannelFactoryMap.put(id, new RpcChannelFactory(id, uri));
    }
    this.requestTimeoutMillis = requestTimeoutMillis;
  }

  public CompletableFuture<ElectVoteResponse> beginElect(int proposerId,
      BallotNumber number,
      LeaderDecree leaderDecree) {
    BeginElectRequest request = new BeginElectRequest(number, leaderDecree);
    if (proposerId == this.proposerId) {
      return localService.beginElect(request);
    }
    return writeRequest(proposerId, PaxosApis.BEGIN_ELECT.code(), request);
  }

  public CompletableFuture<SuccessElectResponse> successElect(int proposerId,
      LeaderDecree leaderDecree) {
    if (proposerId == this.proposerId) {
      return localService.successElect(new SuccessElectRequest(leaderDecree));
    }
    return writeRequest(proposerId, PaxosApis.SUCCESS_ELECT.code(), new SuccessElectRequest(leaderDecree));
  }

  public CompletableFuture<LastVoteResponse> nextBallot(int proposerId, BallotNumber number,
      Long lastDecreeIndex) {
    NextBallotRequest request = new NextBallotRequest(number, lastDecreeIndex);
    if (proposerId == this.proposerId) {
      return localService.nextBallot(request);
    }
    return writeRequest(proposerId, PaxosApis.NEXT_BALLOT.code(), request);
  }

  public CompletableFuture<LastElectResponse> nextElect(int proposerId, BallotNumber number) {
    NextElectRequest request = new NextElectRequest(proposer.getId(), number);
    if (proposerId == this.proposerId) {
      return localService.nextElect(request);
    }
    return writeRequest(proposerId, PaxosApis.NEXT_ELECT.code(), request);
  }

  public CompletableFuture<BallotVoteResponse> beginBallot(int proposerId,
      BallotNumber number,
      GeneralDecree decree
      ) {
    BeginBallotRequest request = new BeginBallotRequest(number, proposer.getDraftPaper().getTerm(),
        decree);
    if (proposerId == this.proposerId) {
      return localService.beginBallot(request);
    }
    return writeRequest(proposerId, PaxosApis.BEGIN_BALLOT.code(), request);
  }

  public CompletableFuture<ExtendLeaderTermResponse> extendLeaderTerm(int proposerId, int leaderId, int term, long ledgerLastIndex) {
    ExtendLeaderTermRequest request = new ExtendLeaderTermRequest(leaderId, term, ledgerLastIndex);
    if (proposerId == this.proposerId) {
      return localService.extendLeaderTerm(request);
    }
    return writeRequest(proposerId, PaxosApis.EXTEND_LEADER_TERM.code(), request);
  }

  public CompletableFuture<RecordDecreeResponse> successDecree(int proposerId,
      RecordDecreeRequest request) {
    if (proposerId == this.proposerId) {
      localService.successDecree(request);
      RecordDecreeResponse recordResp = new RecordDecreeResponse(proposerId,
          request.getDecree().getIndex());
      return CompletableFuture.completedFuture(recordResp);
    }
    return writeRequest(proposerId, PaxosApis.SUCCESS_DECREE.code(), request);
  }

  public CompletableFuture<DecreeReplicateResponse> replicateDecrees(int proposerId,
      long lastIndex) {
    if (proposerId == this.proposerId) {
      throw new IllegalStateException("wrong replicate self");
    }
    return writeRequest(proposerId, PaxosApis.REPLICATE_DECREES.code(), new DecreeReplicateRequest(lastIndex));
  }

  private <T> CompletableFuture<T> writeRequest(int proposerId, String requestCode, Object request) {

    RpcChannelFactory channelFactory = proposerChannelFactoryMap.get(proposerId);
    RpcChannel channel = channelFactory.channel();

    int selfId = proposer.getId();
    ApiRequest apiRequest = new ApiRequest(selfId, requestCode, request);

    CompletableFuture<ApiResponse> responseFuture = channel.write(apiRequest, requestTimeoutMillis);
    CompletableFuture<T> valueFuture = new CompletableFuture<>();
    responseFuture.thenAccept(apiResponse -> {
      valueFuture.complete((T) apiResponse.getBody());
    });
    return valueFuture;
  }

}
