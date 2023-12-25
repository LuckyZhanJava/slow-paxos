package com.lonicera.paxos.core.errors;

public enum Errors {

  NONE(0);

  private int code;

  Errors(int code){
    this.code = code;
  }

  public int code(){
    return code;
  }
}
