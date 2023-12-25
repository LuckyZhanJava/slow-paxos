package com.lonicera.paxos.core.apis;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

@AllArgsConstructor
@NoArgsConstructor
@Data
@ToString
public class ExtendLeaderTermRequest {
  private int leaderId;
  private int term;
  private long lastDecreeIndex;
}
