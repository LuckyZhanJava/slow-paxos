package com.lonicera.paxos.core.apis;

import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class DecreeReplicateResponse {
  private List<GeneralDecree> decreeList;
}
