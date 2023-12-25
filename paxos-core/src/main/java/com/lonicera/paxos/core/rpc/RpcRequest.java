package com.lonicera.paxos.core.rpc;

import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@NoArgsConstructor
public class RpcRequest {


  @Getter
  private long id;
  private int proposerId;
  private String code;
  private byte[] body;


  public RpcRequest(long id, int proposerId, String code, byte[] body) {
    this.id = id;
    this.proposerId = proposerId;
    this.code = code;
    this.body = body;
  }
}
