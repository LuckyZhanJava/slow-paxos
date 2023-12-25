package com.lonicera.paxos.core.protocol;

import com.lonicera.paxos.core.apis.BallotNumber;
import com.lonicera.paxos.core.apis.GeneralDecreeVote;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class DraftPaperObject {
  private int proposerId;
  private int term;//当前任期
  private BallotNumber lastTried; // 发起的最后一个表决
  private GeneralDecreeVote prevGeneralVote; // 最后一个 投票通过的 内容
  private int prevElectTerm; // 最后一个 投票通过的 内容
  private BallotNumber nextBal; // 最后一次 投票通过的 最大编号
  private int logCount;
}
