package com.lonicera.paxos.core.rpc;

import java.util.concurrent.atomic.AtomicLong;
import lombok.Getter;
import lombok.ToString;

@Getter
@ToString
public class ApiRequest {

  private static final AtomicLong idGenerator = new AtomicLong(0);
  private long id;
  private String code;
  private int proposerId;
  private Object body;

  public ApiRequest(int proposerId, String code, Object body) {
    this(idGenerator.incrementAndGet(), proposerId, code, body);
  }

  public ApiRequest(long id, int proposerId, String code, Object body) {
    this.id = id;
    this.code = code;
    this.proposerId = proposerId;
    this.body = body;
  }

}
