package com.lonicera.paxos.core.rpc;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class RpcResponse {
  private long id;
  private int errorCode;
  private byte[] body;
}
