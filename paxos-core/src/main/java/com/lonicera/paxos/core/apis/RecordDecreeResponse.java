package com.lonicera.paxos.core.apis;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class RecordDecreeResponse {
  private int proposerId;
  private long lastDecreeIndex;
}
