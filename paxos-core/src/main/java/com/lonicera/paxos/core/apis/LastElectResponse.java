package com.lonicera.paxos.core.apis;

import java.io.Serializable;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

@Data
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class LastElectResponse implements Serializable {
  private boolean accept;
  private int proposerId;
  private BallotNumber nextBal;
  private long lastDecreeIndex;
  private int prevElectTerm;
}
