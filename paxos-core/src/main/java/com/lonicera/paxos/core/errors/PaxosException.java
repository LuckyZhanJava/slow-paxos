package com.lonicera.paxos.core.errors;


public class PaxosException extends RuntimeException {

  public PaxosException(String msg){
    super(msg);
  }

  public PaxosException(Throwable t){
    super(t);
  }
}
