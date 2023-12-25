package com.lonicera.paxos.core.apis;

import java.io.Serializable;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class ElectVoteResponse implements Serializable {
  private boolean accept;
  private int proposerId;
}
