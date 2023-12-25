package com.lonicera.paxos.core.server;


import com.lonicera.paxos.core.apis.BallotVoteResponse;
import com.lonicera.paxos.core.apis.BeginBallotRequest;
import com.lonicera.paxos.core.apis.BeginElectRequest;
import com.lonicera.paxos.core.apis.DecreeReplicateRequest;
import com.lonicera.paxos.core.apis.DecreeReplicateResponse;
import com.lonicera.paxos.core.apis.ElectVoteResponse;
import com.lonicera.paxos.core.apis.ExtendLeaderTermRequest;
import com.lonicera.paxos.core.apis.ExtendLeaderTermResponse;
import com.lonicera.paxos.core.apis.LastElectResponse;
import com.lonicera.paxos.core.apis.LastVoteResponse;
import com.lonicera.paxos.core.apis.NextBallotRequest;
import com.lonicera.paxos.core.apis.NextElectRequest;
import com.lonicera.paxos.core.apis.PaxosApis;
import com.lonicera.paxos.core.apis.RecordDecreeRequest;
import com.lonicera.paxos.core.apis.RecordDecreeResponse;
import com.lonicera.paxos.core.apis.SuccessElectRequest;
import com.lonicera.paxos.core.apis.SuccessElectResponse;
import com.lonicera.paxos.core.codec.ObjectCodec;
import com.lonicera.paxos.core.errors.Errors;
import com.lonicera.paxos.core.protocol.Proposer;
import com.lonicera.paxos.core.rpc.ApiRequest;
import com.lonicera.paxos.core.rpc.ApiResponse;
import com.lonicera.paxos.core.rpc.RpcRequest;
import com.lonicera.paxos.core.rpc.RpcResponse;
import com.lonicera.paxos.core.service.BallotService;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ClusterApiRequestProcessHandler extends ChannelInboundHandlerAdapter {

  private Proposer proposer;
  private BallotService ballotService;

  public ClusterApiRequestProcessHandler(Proposer proposer) {
    this.proposer = proposer;
    this.ballotService = new BallotService(proposer);
  }

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
    if (msg instanceof ByteBuf) {
      byte[] bytes = ByteBufUtil.getBytes((ByteBuf) msg);
      ((ByteBuf) msg).release();
      RpcRequest rpcRequest = ObjectCodec.codec().decode(bytes, RpcRequest.class);
      Class<?> clazz = PaxosApis.requestType(rpcRequest.getCode());
      Object bodyObject = ObjectCodec.codec().decode(rpcRequest.getBody(), clazz);
      ApiRequest apiRequest = new ApiRequest(rpcRequest.getId(), rpcRequest.getProposerId(),
          rpcRequest.getCode(),
          bodyObject);
      channelRead0(ctx, apiRequest);
    } else {
      ctx.fireChannelRead(msg);
    }
  }


  public void channelRead0(ChannelHandlerContext ctx, ApiRequest request) {
    int proposerId = request.getProposerId();
    if (!proposer.getProposerUriConfig().containsKey(proposerId)) {
      log.warn("Read request from {} not member in cluster", proposerId);
      return;
    }
    String code = request.getCode();
    PaxosApis api = PaxosApis.apiOf(code);
    switch (api) {
      case NEXT_BALLOT: {
        NextBallotRequest nextBallotRequest = (NextBallotRequest) request.getBody();
        CompletableFuture<LastVoteResponse> lastVoteFuture = ballotService
            .nextBallot(nextBallotRequest);
        lastVoteFuture.thenAccept((lastVote) -> {
          writeResponse(ctx, request, lastVote);
        });
        break;
      }
      case BEGIN_BALLOT: {
        BeginBallotRequest beginBallotRequest = (BeginBallotRequest) request.getBody();
        CompletableFuture<BallotVoteResponse> votedFuture = ballotService
            .beginBallot(beginBallotRequest);
        votedFuture.thenAccept((voted) -> {
          writeResponse(ctx, request, voted);
        });
        break;
      }
      case SUCCESS_DECREE: {
        RecordDecreeRequest generalDecree = (RecordDecreeRequest) (request.getBody());
        CompletableFuture<RecordDecreeResponse> recordFuture = ballotService
            .successDecree(generalDecree);
        recordFuture.thenAccept((record) -> {
          writeResponse(ctx, request, record);
        });
        break;
      }
      case NEXT_ELECT: {
        NextElectRequest nextElectRequest = (NextElectRequest) (request.getBody());
        CompletableFuture<LastElectResponse> lastVoteFuture = ballotService
            .nextElect(nextElectRequest);
        lastVoteFuture.thenAccept((lastElect) -> {
          writeResponse(ctx, request, lastElect);
        });
        break;
      }
      case BEGIN_ELECT: {
        BeginElectRequest beginElectRequest = (BeginElectRequest) (request.getBody());
        CompletableFuture<ElectVoteResponse> votedFuture = ballotService
            .beginElect(beginElectRequest);
        votedFuture.thenAccept((voted) -> {
          writeResponse(ctx, request, voted);
        });
        break;
      }
      case SUCCESS_ELECT: {
        SuccessElectRequest req = (SuccessElectRequest) (request.getBody());
        CompletableFuture<SuccessElectResponse> leaderIdFuture = ballotService
            .successElect(req);
        leaderIdFuture.thenAccept((resp) -> {
          writeResponse(ctx, request, resp);
        });
        break;
      }
      case EXTEND_LEADER_TERM: {
        ExtendLeaderTermRequest req = (ExtendLeaderTermRequest) (request.getBody());
        CompletableFuture<ExtendLeaderTermResponse> leaderIdFuture = ballotService
            .extendLeaderTerm(req);
        leaderIdFuture.thenAccept((resp) -> {
          writeResponse(ctx, request, resp);
        });
        break;
      }
      case REPLICATE_DECREES: {
        DecreeReplicateRequest req = (DecreeReplicateRequest) (request.getBody());
        long lastIndex = req.getLastDecreeIndex();
        CompletableFuture<DecreeReplicateResponse> syncFuture = ballotService
            .ledgerDecreeList(lastIndex);
        syncFuture.thenAccept((resp) -> {
          writeResponse(ctx, request, resp);
        });
        break;
      }
    }
  }


  private void writeResponse(ChannelHandlerContext ctx, ApiRequest request,
      Object resp) {
    if (resp == null) {
      return;
    }
    long id = request.getId();
    ApiResponse response = new ApiResponse(id, Errors.NONE.code(), resp);
    byte[] bytes = ObjectCodec.codec().encode(response.getBody());
    RpcResponse rpcResponse = new RpcResponse(response.getId(), response.getErrorCode(), bytes);
    byte[] respBytes = ObjectCodec.codec().encode(rpcResponse);
    ByteBuf byteBuf = ctx.channel().alloc().buffer();
    byteBuf.writeBytes(respBytes);
    ctx.writeAndFlush(byteBuf);
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
    if (cause instanceof IOException) {
      log.info("channel : {} error will be close", ctx.channel());
    } else {
      super.exceptionCaught(ctx, cause);
    }
  }
}
