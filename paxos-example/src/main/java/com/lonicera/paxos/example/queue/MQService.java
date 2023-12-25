package com.lonicera.paxos.example.queue;


import com.baidu.bjf.remoting.protobuf.annotation.ProtobufClass;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.ToString;

public interface MQService {
  PutResult queue(Message message) throws Exception;

  @NoArgsConstructor
  @AllArgsConstructor
  @ToString
  @ProtobufClass
  class PutResult{
    private long index;
  }
}
