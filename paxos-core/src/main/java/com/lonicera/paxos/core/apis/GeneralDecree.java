package com.lonicera.paxos.core.apis;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class GeneralDecree {
  private long index;
  private byte[] vote;
  private int term;
}
