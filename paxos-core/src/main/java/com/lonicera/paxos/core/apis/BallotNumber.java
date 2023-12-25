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
public class BallotNumber implements Serializable {

  private long number;

  public int compareTo(BallotNumber another) {
    return (int)(number - another.number);
  }

  public BallotNumber increase(){
    return new BallotNumber(number + 1);
  }
}
