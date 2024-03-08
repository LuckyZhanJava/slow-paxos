package com.lonicera.paxos.core.protocol;

import com.google.common.collect.Maps;
import com.lonicera.paxos.core.apis.BallotNumber;
import com.lonicera.paxos.core.apis.BallotVoteResponse;
import com.lonicera.paxos.core.apis.DecreeReplicateResponse;
import com.lonicera.paxos.core.apis.ElectVoteResponse;
import com.lonicera.paxos.core.apis.GeneralDecree;
import com.lonicera.paxos.core.apis.LastElectResponse;
import com.lonicera.paxos.core.apis.LastVoteResponse;
import com.lonicera.paxos.core.apis.LeaderDecree;
import com.lonicera.paxos.core.apis.RecordDecreeRequest;
import com.lonicera.paxos.core.apis.RecordDecreeResponse;
import com.lonicera.paxos.core.apis.SuccessElectResponse;
import com.lonicera.paxos.core.errors.NotLeaderException;
import com.lonicera.paxos.core.errors.PaxosException;
import com.lonicera.paxos.core.server.ClusterRpcServer;
import com.lonicera.paxos.core.server.ProposerConfig;
import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Proposer {

  public enum State {
    STATE_CANDIDATE,
    STATE_FOLLOWER,
    STATE_LEADER_SYNCING,
    STATE_LEADER_SYNCED,
  }

  @Getter
  private int id;
  @Getter
  private volatile State state;
  private final Object stateLock = new Object();
  private String dataDir;
  @Getter
  private DraftPaper draftPaper;
  @Getter
  private Ledger ledger;
  @Getter
  private Messenger messenger;
  private LawBook lawBook;
  private int leaderId = -1;
  private Future<?> electLeaderTask;
  private Future<?> extendTermTask;
  private ClusterRpcServer clusterRpcServer;
  private ScheduledExecutorService scheduledExecutor;
  @Getter
  private Map<Integer, String> proposerUriConfig;

  public Proposer(
      ProposerConfig config,
      ProposerReader reader
  ) throws IOException {
    this.id = config.getId();
    this.state = State.STATE_CANDIDATE;
    this.dataDir = config.getDataDir();
    this.draftPaper = DraftPaper.read(dataDir, id);
    this.proposerUriConfig = config.getProposerUriConfig();
    File dataDirFile = new File(dataDir);
    if (!dataDirFile.exists()) {
      dataDirFile.mkdirs();
    }
    this.lawBook = reader.readLawBook(dataDir);
    this.ledger = reader.readLedger(dataDir);
    messenger = new Messenger(
        this,
        proposerUriConfig,
        config.getRequestTimeoutMillis()
    );
    scheduledExecutor = Executors
        .newScheduledThreadPool(Runtime.getRuntime().availableProcessors() * 2);
    clusterRpcServer = new ClusterRpcServer(this, proposerUriConfig.get(id));
  }

  public void start() throws Exception {
    lawBook = induceLedger(lawBook, ledger);
    clusterRpcServer.start();
    log.info("Proposer : {} server started", id);
    becomeCandidate();
  }

  public void record(List<GeneralDecree> decreeList) {
    for (GeneralDecree decree : decreeList) {
      record(decree);
    }
  }

  public synchronized void record(GeneralDecree decree) {
    ledger.record(decree);
    lawBook.induce(decree);
  }

  public int getLeaderId() {
    return leaderId;
  }


  private void lookForLeader(int id, DraftPaper draftPaper, Ledger ledger) {

    if (state != State.STATE_CANDIDATE) {
      return;
    }
    log.info("Node : {} start looking leader", id);
    BallotNumber lastTried = draftPaper.getLastTried();
    BallotNumber newTry = lastTried.increase();
    draftPaper.setLastTried(newTry);

    Set<Integer> proposerIdSet = proposerUriConfig.keySet();

    int quorumCount = proposerUriConfig.size() / 2 + 1;
    Quorum quorum = new Quorum(quorumCount);
    Map<Integer, LastElectResponse> quorumVoteMap = Maps.newHashMap();

    log.info("Node : {} start next elect", id);
    for (int proposerId : proposerIdSet) {
      CompletableFuture<LastElectResponse> lastElect = messenger
          .nextElect(proposerId, newTry);
      lastElect.thenAccept((vote) -> {
            if (vote.isAccept()) {
              quorum.join(proposerId);
              quorumVoteMap.put(proposerId, vote);
            } else {
              if (vote.getNextBal().compareTo(newTry) > 0) {
                if(draftPaper.setLastTried(vote.getNextBal())){
                  log.info("Node : {} nextBal : {} update to : {}", id, newTry, vote.getNextBal());
                }
              }
            }
          }
      );
    }

    quorum.waitQuorate(5000);

    if (!quorum.isQuorate()) {
      randomSleep(5);
      log.info("Node : {} next elect timeout", id);
      lookForLeader(id, draftPaper, ledger);
      return;
    }

    Collection<LastElectResponse> quorumVoteList = quorumVoteMap.values();

    int expectLeaderId = longestLedgerProposerId(quorumVoteList);

    if (expectLeaderId != id) {
      log.info("Node : {} not win, leader id : {}", id, expectLeaderId);
      sleep(10);
      lookForLeader(id, draftPaper, ledger);
      return;
    }

    int maxPrevElectTerm = maxPrevElectTerm(quorumVoteList);

    LeaderDecree leaderDecree = new LeaderDecree(maxPrevElectTerm + 1, ledger.lastDecreeIndex(),
        id);

    quorum.reset();

    log.info("Node : {} begin elect : {}", id, leaderDecree);
    for (int proposerId : quorumVoteMap.keySet()) {
      CompletableFuture<ElectVoteResponse> voteFuture = messenger
          .beginElect(proposerId, newTry, leaderDecree);
      voteFuture.thenAccept(vote -> {
        if (vote.isAccept()) {
          quorum.join(proposerId);
        }
      });
    }

    quorum.waitQuorate(5000);

    if (!quorum.isQuorate()) {
      log.info("Node : {} elect timeout will sleep", id);
      randomSleep(10);
      lookForLeader(id, draftPaper, ledger);
      return;
    }

    log.info("Node : {} success elect", id);
    for (Integer proposerId : proposerIdSet) {
      CompletableFuture<SuccessElectResponse> electRespFuture = messenger
          .successElect(proposerId, leaderDecree);
    }
    if (state == State.STATE_CANDIDATE) {
      lookForLeader(id, draftPaper, ledger);
    }
  }

  private void randomSleep(int second) {
    sleep(ThreadLocalRandom.current().nextInt(second));
  }

  private int maxPrevElectTerm(Collection<LastElectResponse> voteList) {
    LastElectResponse maxVote = Collections.max(voteList, (v1, v2) -> {
      if (v1.getPrevElectTerm() != v2.getPrevElectTerm()) {
        return v1.getPrevElectTerm() - v2.getPrevElectTerm();
      }
      return v1.getProposerId() - v2.getProposerId();
    });
    return maxVote.getPrevElectTerm();
  }

  private void sleep(long timeoutSecond) {
    try {
      TimeUnit.SECONDS.sleep(timeoutSecond);
    } catch (InterruptedException e) {

    }
  }

  private int longestLedgerProposerId(Collection<LastElectResponse> voteList) {
    LastElectResponse maxVote = Collections.max(voteList, (v1, v2) -> {
      if (v1.getLastDecreeIndex() != v2.getLastDecreeIndex()) {
        return (int) (v1.getLastDecreeIndex() - v2.getLastDecreeIndex());
      }
      return v1.getProposerId() - v2.getProposerId();
    });
    return maxVote.getProposerId();
  }


  private LawBook induceLedger(LawBook lawBook, Ledger ledger) {
    Iterator<GeneralDecree> decreeItr = ledger.decreeItr(lawBook.lastDecreeIndex());
    while (decreeItr.hasNext()) {
      GeneralDecree decree = decreeItr.next();
      lawBook.induce(decree);
    }
    return lawBook;
  }

  public void shutdown() throws IOException {
    clusterRpcServer.shutdown();
    scheduledExecutor.shutdown();
    synchronized (draftPaper) {
      draftPaper.close();
      ledger.close();
      lawBook.close();
    }
  }

  public void becomeLeader(int term) {
    synchronized (stateLock) {

      setLeaderId(id);
      draftPaper.setTerm(term);
      log.info("Node : {}  become Leader, Term : {}", id, term);
      cancelAndScheduleNewElect();
      setState(State.STATE_LEADER_SYNCING);
      scheduleLeaderSyncPropose();
      scheduleExtendLeaderTerm(leaderId, term, ledger);
    }
  }

  private void scheduleLeaderSyncPropose() {
    scheduledExecutor.submit(new Runnable() {
      @Override
      public void run() {
        try {
          leaderSyncPropose();
          synchronized (stateLock) {
            if (state == State.STATE_LEADER_SYNCING) {
              setState(State.STATE_LEADER_SYNCED);
              stateLock.notifyAll();
            }
          }
        } catch (Throwable t) {
          log.error("leader sync error {} become to candidate", t);
          becomeCandidate();
        }
      }
    });
  }

  private void setState(State state) {
    this.state = state;
  }

  private void leaderSyncPropose() {
    if (state == State.STATE_CANDIDATE || state == State.STATE_FOLLOWER) {
      return;
    }
    BallotNumber lastTried = draftPaper.getLastTried();
    BallotNumber newTried = lastTried.increase();
    draftPaper.setLastTried(newTried);
    int proposerCount = proposerUriConfig.size();
    Quorum joinQuorum = new Quorum(proposerCount / 2 + 1);

    Map<Integer, LastVoteResponse> proposerVoteMap = new HashMap<>();
    for (int proposerId : proposerUriConfig.keySet()) {
      CompletableFuture<LastVoteResponse> respFuture = messenger
          .nextBallot(proposerId, newTried, ledger.lastDecreeIndex());
      respFuture.thenAccept((resp) -> {
        if (resp.isAccept()) {
          boolean join = joinQuorum.join(proposerId);
          if (join) {
            proposerVoteMap.put(proposerId, resp);
          }
        } else {
          long newLastDecreeIndex = resp.getLastDecreeIndex();
          if (newLastDecreeIndex > ledger.lastDecreeIndex()) {
            List<GeneralDecree> decreeList = resp.getDecreeList();
            record(decreeList);
          }
          if (resp.getNextBal().compareTo(newTried) > 0) {
            draftPaper.setLastTried(resp.getNextBal());
          }
        }
      });
    }

    joinQuorum.waitQuorate(5000);

    int quorumCount = proposerCount / 2 + 1;
    if (proposerVoteMap.size() < quorumCount) {
      leaderSyncPropose();
      return;
    }

    LastVoteResponse prevVoteResp = latestPrevVote(proposerVoteMap.values());
    if (prevVoteResp.getPrevVote() == null) {
      return;
    }
    GeneralDecree prevVote = prevVoteResp.getPrevVote().getDecree();

    if (prevVote.getIndex() == ledger.lastDecreeIndex()) {
      return;
    }

    if (prevVote.getIndex() - ledger.lastDecreeIndex() > 1) {
      throw new IllegalStateException("Leader is not the latest one");
    }

    joinQuorum.reset();
    for (Integer proposerId : proposerVoteMap.keySet()) {
      CompletableFuture<BallotVoteResponse> voteResp = messenger
          .beginBallot(proposerId, newTried, prevVote);
      voteResp.thenAccept(vote -> {
        if (vote.isAccept()) {
          joinQuorum.join(vote.getProposerId());
        }
      });
    }

    joinQuorum.waitQuorate(10000);

    if (joinQuorum.isQuorate()) {
      return;
    } else {
      leaderSyncPropose();
    }

    RecordDecreeRequest request = new RecordDecreeRequest(prevVote);
    for (Integer proposerId : proposerVoteMap.keySet()) {
      CompletableFuture<RecordDecreeResponse> recordResp = messenger
          .successDecree(proposerId, request);
    }

    if (prevVote.getIndex() == ledger.lastDecreeIndex()) {
      return;
    }

    leaderSyncPropose();


  }

  private LastVoteResponse latestPrevVote(Collection<LastVoteResponse> voteList) {
    LastVoteResponse latestPrevVoteResp = Collections.max(voteList, (o1, o2) -> {
      if (o1.getPrevVote() == null) {
        return -1;
      }
      if (o2.getPrevVote() == null) {
        return 1;
      }
      return o1.getPrevVote().getNumber().compareTo(o2.getPrevVote().getNumber());
    });
    return latestPrevVoteResp;
  }

  private void scheduleExtendLeaderTerm(int leaderId, int term, Ledger ledger) {
    log.info("Extend leader term => LeaderId : {} , Term : {}", leaderId, term);
    extendTermTask = scheduledExecutor.schedule(() -> {
          if (state == State.STATE_LEADER_SYNCING || state == State.STATE_LEADER_SYNCED) {
            extendLeaderTerm(leaderId, term, ledger.lastDecreeIndex());
            scheduleExtendLeaderTerm(leaderId, term, ledger);
          }
        },
        10,
        TimeUnit.SECONDS
    );
  }

  private void extendLeaderTerm(int leaderId, int term, long ledgerLastIndex) {
    for (int proposerId : proposerUriConfig.keySet()) {
      messenger.extendLeaderTerm(proposerId, leaderId, term, ledgerLastIndex);
    }
  }

  public void cancelAndScheduleNewElect() {
    if (electLeaderTask != null) {
      log.info("Node(id:{}, state:{}, ledgerIndex:{}->{}) cancel new elect", id, state,
          ledger.firstDecreeIndex(), ledger.lastDecreeIndex());
      electLeaderTask.cancel(true);
    }
    int random = ThreadLocalRandom.current().nextInt(5);
    electLeaderTask = scheduledExecutor.schedule(() -> {
          log.info("Node state : {} start new elect", state);
          becomeCandidate();
        },
        20 + random,
        TimeUnit.SECONDS
    );
  }

  private void becomeCandidate() {
    synchronized (stateLock) {
      setLeaderId(-1);
      cancelExtendTermTask();
      setState(State.STATE_CANDIDATE);
      stateLock.notifyAll();
      scheduledExecutor.submit(() -> lookForLeader(id, draftPaper, ledger));
    }
  }

  private void setLeaderId(int leaderId) {
    this.leaderId = leaderId;
  }

  private void cancelExtendTermTask() {
    if (extendTermTask != null) {
      log.info("Node : {} state : {} cancel extend term", id, state);
      extendTermTask.cancel(true);
    }
  }

  public void becomeFollow(int leaderId, int term, long leaderLastIndex) {
    synchronized (stateLock) {
      cancelExtendTermTask();
      cancelAndScheduleNewElect();
      draftPaper.setTerm(term);
      setLeaderId(leaderId);
      setState(State.STATE_FOLLOWER);
      stateLock.notifyAll();
      log.info("Node : {}  become Follower, Leader Id : {} , Term : {}", id, leaderId, term);
      long followerLastIndex = ledger.lastDecreeIndex();
      if (followerLastIndex < leaderLastIndex) {
        scheduleFollowerSyncLeader(leaderLastIndex);
      }
    }
  }

  private void scheduleFollowerSyncLeader(long leaderLastIndex) {
    scheduledExecutor.submit(new Runnable() {
      @Override
      public void run() {
        log.info(
            "Node(id:{}, state:{}, ledgerIndex:{}->{}) start sync with Leader(id:{}, ledgerIndex:{})",
            id, state, ledger.firstDecreeIndex(), ledger.lastDecreeIndex(), leaderId,
            leaderLastIndex);
        CompletableFuture<DecreeReplicateResponse> respList = messenger
            .replicateDecrees(leaderId, ledger.lastDecreeIndex());
        respList.thenApply((resp) -> {
          List<GeneralDecree> decreeList = resp.getDecreeList();
          record(decreeList);
          return ledger.lastDecreeIndex();
        }).thenAccept((newIndex) -> {
          if (newIndex < leaderLastIndex) {
            scheduleFollowerSyncLeader(leaderLastIndex);
          }
        })
        ;
      }
    });
  }

  public long propose(byte[] bytes) throws NotLeaderException {
    synchronized (stateLock) {
      if (state == State.STATE_LEADER_SYNCED) {
        long start = System.nanoTime();
        long index = doPropose(bytes);
        long end = System.nanoTime();
        log.info("propose cost : {}", end - start);
        return index;
      }
      while (state == State.STATE_LEADER_SYNCING) {
        try {
          stateLock.wait();
        } catch (InterruptedException e) {
          //ignore
        }
      }
      if (state == State.STATE_CANDIDATE || state == State.STATE_FOLLOWER) {
        throw new NotLeaderException(state, leaderId);
      }
      return propose(bytes);
    }
  }

  private long doPropose(byte[] bytes) throws NotLeaderException {

    if (state == State.STATE_LEADER_SYNCING) {
      throw new IllegalStateException("leader not synced");
    }

    if (state != State.STATE_LEADER_SYNCED) {
      throw new NotLeaderException(state, leaderId);
    }

    long lastIndex = ledger.lastDecreeIndex();
    long nextIndex = lastIndex + 1;

    Set<Integer> proposerIdSet = proposerUriConfig.keySet();

    GeneralDecree decree = new GeneralDecree(nextIndex, bytes, draftPaper.getTerm());

    BallotNumber lastTried = draftPaper.getLastTried();
    BallotNumber newTried = lastTried.increase();

    draftPaper.setLastTried(newTried);

    Quorum quorum = new Quorum(proposerIdSet.size() / 2 + 1);

    long start = System.nanoTime();
    for (Integer proposerId : proposerIdSet) {
      CompletableFuture<BallotVoteResponse> voteResp = messenger
          .beginBallot(proposerId, draftPaper.getLastTried(), decree);
      voteResp.thenAccept(vote -> {
        if (vote.isAccept()) {
          quorum.join(vote.getProposerId());
        }
      });
    }

    quorum.waitQuorate(5000);

    if (!quorum.isQuorate()) {
      scheduleLeaderSyncPropose();
      throw new PaxosException("Leader propose timeout");
    }

    long end = System.nanoTime();
    log.info("ballot cost : {}", end - start);

    start = System.nanoTime();
    RecordDecreeRequest request = new RecordDecreeRequest(decree);
    for (Integer proposerId : proposerIdSet) {
      CompletableFuture<RecordDecreeResponse> voteResp = messenger
          .successDecree(proposerId, request);
    }
    end = System.nanoTime();
    log.info("success cost : {}", end - start);

    if (ledger.lastDecreeIndex() == nextIndex) {
      return nextIndex;
    }

    return doPropose(bytes);
  }
}
