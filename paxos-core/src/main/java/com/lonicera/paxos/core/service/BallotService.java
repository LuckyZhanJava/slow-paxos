package com.lonicera.paxos.core.service;

import com.google.common.collect.Lists;
import com.lonicera.paxos.core.apis.BallotNumber;
import com.lonicera.paxos.core.apis.BallotVoteResponse;
import com.lonicera.paxos.core.apis.BeginBallotRequest;
import com.lonicera.paxos.core.apis.BeginElectRequest;
import com.lonicera.paxos.core.apis.DecreeReplicateResponse;
import com.lonicera.paxos.core.apis.ElectVoteResponse;
import com.lonicera.paxos.core.apis.ExtendLeaderTermRequest;
import com.lonicera.paxos.core.apis.ExtendLeaderTermResponse;
import com.lonicera.paxos.core.apis.GeneralDecree;
import com.lonicera.paxos.core.apis.GeneralDecreeVote;
import com.lonicera.paxos.core.apis.LastElectResponse;
import com.lonicera.paxos.core.apis.LastVoteResponse;
import com.lonicera.paxos.core.apis.LeaderDecree;
import com.lonicera.paxos.core.apis.NextBallotRequest;
import com.lonicera.paxos.core.apis.NextElectRequest;
import com.lonicera.paxos.core.apis.RecordDecreeRequest;
import com.lonicera.paxos.core.apis.RecordDecreeResponse;
import com.lonicera.paxos.core.apis.SuccessElectRequest;
import com.lonicera.paxos.core.apis.SuccessElectResponse;
import com.lonicera.paxos.core.protocol.Proposer;
import com.lonicera.paxos.core.protocol.Proposer.State;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class BallotService {

  public Proposer proposer;

  public BallotService(Proposer proposer) {
    this.proposer = proposer;
  }

  public CompletableFuture<ElectVoteResponse> beginElect(BeginElectRequest request) {
    BallotNumber reqNextBal = request.getNumber();
    int proposerId = proposer.getId();

    if (proposer.getDraftPaper().getNextBal().compareTo(reqNextBal) != 0
        || proposer.getDraftPaper().getTerm() > request.getDecree().getNewTerm()
        || proposer.getLedger().lastDecreeIndex() > request.getDecree().getLastDecreeIndex()
    ) {
      ElectVoteResponse rejectVote = new ElectVoteResponse(false, proposerId);
      return CompletableFuture.completedFuture(rejectVote);
    }

    ElectVoteResponse acceptVote = new ElectVoteResponse(true, proposerId);
    proposer.getDraftPaper().setPrevElectTerm(request.getDecree().getNewTerm());
    return CompletableFuture.completedFuture(acceptVote);

  }


  public CompletableFuture<SuccessElectResponse> successElect(SuccessElectRequest req) {
    LeaderDecree leaderDecree = req.getLeaderDecree();
    int newTerm = leaderDecree.getNewTerm();
    if (newTerm <= proposer.getDraftPaper().getTerm()) {
      return new CompletableFuture();
    }
    if (leaderDecree.getLeaderId() == proposer.getId()) {
      proposer.becomeLeader(newTerm);
    } else {
      int leaderId = leaderDecree.getLeaderId();
      proposer.becomeFollow(leaderId, newTerm, leaderDecree.getLastDecreeIndex());
    }
    return CompletableFuture.completedFuture(new SuccessElectResponse(proposer.getId()));
  }


  public CompletableFuture<LastVoteResponse> nextBallot(NextBallotRequest request) {
    BallotNumber reqNewBal = request.getNumber();
    BallotNumber nextBal = proposer.getDraftPaper().getNextBal();

    boolean accept = true;
    if (reqNewBal.compareTo(nextBal) > 0) {
      if (!proposer.getDraftPaper().setNextBal(reqNewBal)) {
        accept = false;
      }
    } else {
      accept = false;
    }

    long requestLastIndex = request.getLastDecreeIndex();
    long selfLastIndex = proposer.getLedger().lastDecreeIndex();

    List<GeneralDecree> decreeList = new LinkedList<>();
    if (selfLastIndex > requestLastIndex) {
      accept = false;
      Iterator<GeneralDecree> decreeItr = proposer.getLedger().decreeItr(requestLastIndex + 1);
      while (decreeItr.hasNext()) {
        decreeList.add(decreeItr.next());
      }
    }

    LastVoteResponse lastVoteResponse = new LastVoteResponse(
        accept,
        proposer.getId(),
        proposer.getDraftPaper().getNextBal(),
        proposer.getLedger().lastDecreeIndex(),
        decreeList,
        proposer.getDraftPaper().getPrevGeneralVote()
    );
    return CompletableFuture.completedFuture(lastVoteResponse);
  }

  public CompletableFuture<LastElectResponse> nextElect(NextElectRequest request) {
    BallotNumber reqNewBal = request.getNumber();
    BallotNumber nextBal = proposer.getDraftPaper().getNextBal();

    boolean accept = true;

    if (reqNewBal.compareTo(nextBal) > 0) {

      if (proposer.getDraftPaper().setNextBal(reqNewBal)) {
        log.info("Node : {} update nextBal({}) from Node : {}", proposer.getId(), reqNewBal,
            request.getProposerId());
      }else{
        accept = false;
        log.info("Node : {} reject nextBal({}) from Node : {}", proposer.getId(), reqNewBal,
            request.getProposerId());
      }
    } else {
      accept = false;
      log.info("Node : {} reject nextBal({}) from Node : {}", proposer.getId(), reqNewBal,
          request.getProposerId());
    }

    if (!proposer.getState().equals(State.STATE_CANDIDATE)) {
      return new CompletableFuture<>();
    }

    LastElectResponse lastVoteResponse = new LastElectResponse(
        accept,
        proposer.getId(),
        proposer.getDraftPaper().getNextBal(),
        proposer.getLedger().lastDecreeIndex(),
        proposer.getDraftPaper().getPrevElectTerm()
    );

    return CompletableFuture.completedFuture(lastVoteResponse);
  }


  public CompletableFuture<BallotVoteResponse> beginBallot(BeginBallotRequest request) {
    int proposerId = proposer.getId();

    if (proposer.getState() == State.STATE_CANDIDATE) {
      return CompletableFuture.completedFuture(
          new BallotVoteResponse(false, proposerId));
    }

    BallotNumber reqNumber = request.getNumber();

    if (proposer.getDraftPaper().getNextBal().compareTo(reqNumber) <= 0
        && proposer.getDraftPaper().getTerm() == request.getTerm()) {

      BallotVoteResponse acceptVote = new BallotVoteResponse(true, proposerId);

      GeneralDecreeVote decreeVote = new GeneralDecreeVote(reqNumber, request.getDecree());
      proposer.getDraftPaper().setPrevGeneralVote(decreeVote);

      log.info("Node : {} accept ballot {} decree {}", proposer.getId(), reqNumber,
          request.getDecree().getIndex());

      return CompletableFuture.completedFuture(acceptVote);

    }

    return CompletableFuture.completedFuture(
        new BallotVoteResponse(false, proposerId));
  }


  public CompletableFuture<RecordDecreeResponse> successDecree(
      RecordDecreeRequest req) {
    long lastIndex = proposer.getLedger().lastDecreeIndex();
    if (lastIndex < 0 || req.getDecree().getIndex() - lastIndex == 1) {
      GeneralDecree decree = req.getDecree();
      proposer.record(decree);
      log.info("Node : {} record decree : {}", proposer.getId(), decree.getIndex());
      RecordDecreeResponse recordResult = new RecordDecreeResponse(proposer.getId(),
          decree.getIndex());
      return CompletableFuture.completedFuture(recordResult);
    } else {
      CompletableFuture<DecreeReplicateResponse> decreeListResp = proposer.getMessenger()
          .replicateDecrees(proposer.getLeaderId(), lastIndex);

      CompletableFuture<RecordDecreeResponse> future = new CompletableFuture();

      decreeListResp.thenAccept((resp) -> {
        List<GeneralDecree> recordList = resp.getDecreeList();
        for (GeneralDecree record : recordList) {
          proposer.record(record);
        }
        successDecree(req).thenAccept((recordResp) -> {
          future.complete(recordResp);
        });
      });

      return future;
    }
  }


  public CompletableFuture<ExtendLeaderTermResponse> extendLeaderTerm(
      ExtendLeaderTermRequest request) {
    int term = request.getTerm();
    int leaderId = request.getLeaderId();

    if (proposer.getDraftPaper().getTerm() > term) {
      return new CompletableFuture<>();
    }

    CompletableFuture respFuture = CompletableFuture
        .completedFuture(new ExtendLeaderTermResponse(proposer.getId()));

    if (proposer.getDraftPaper().getTerm() < term) {
      proposer.becomeFollow(leaderId, term, request.getLastDecreeIndex());
      return respFuture;
    }

    if (proposer.getState() == State.STATE_CANDIDATE) {
      proposer.becomeFollow(leaderId, term, request.getLastDecreeIndex());
      return respFuture;
    }

    proposer.cancelAndScheduleNewElect();
    return respFuture;
  }

  public CompletableFuture<DecreeReplicateResponse> ledgerDecreeList(Long lastIndex) {
    Iterator<GeneralDecree> decreeItr = proposer.getLedger().decreeItr(lastIndex + 1);
    DecreeReplicateResponse resp = new DecreeReplicateResponse(Lists.newArrayList(decreeItr));
    return CompletableFuture.completedFuture(resp);
  }
}
