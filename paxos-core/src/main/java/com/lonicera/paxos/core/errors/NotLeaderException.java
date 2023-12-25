package com.lonicera.paxos.core.errors;

import com.lonicera.paxos.core.protocol.Proposer.State;
import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public class NotLeaderException extends Exception {
  private State state;
  private int leaderId;

}
