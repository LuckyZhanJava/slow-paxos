package com.lonicera.paxos.core.protocol;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class Quorum {

  private final int quorumCount;
  private Set<Integer> proposerIdSet;
  private boolean abort = false;

  public Quorum(int quorumCount) {
    this.quorumCount = quorumCount;
    proposerIdSet = new HashSet<>();
  }

  public synchronized boolean join(int proposerId) {
    if (abort) {
      return true;
    }
    if (proposerIdSet.size() >= quorumCount) {
      return false;
    }
    proposerIdSet.add(proposerId);
    if (proposerIdSet.size() >= quorumCount) {
      notifyAll();
    }
    return true;
  }

  public synchronized void waitQuorate(long timeoutMillis) {
    long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
    while (!abort && !isQuorate() && System.nanoTime() < deadline && timeoutMillis > 0) {
      try {
        wait(timeoutMillis);
      } catch (InterruptedException e) {
        //ignore interrupt
      }
      timeoutMillis = TimeUnit.NANOSECONDS.toMillis(deadline - System.nanoTime());
    }
    return;
  }

  public synchronized boolean isQuorate() {
    return proposerIdSet.size() >= quorumCount;
  }

  public synchronized void reset() {
    proposerIdSet = new HashSet<>();
    abort = false;
  }

  public synchronized void abort() {
    abort = true;
    notifyAll();
  }
}
