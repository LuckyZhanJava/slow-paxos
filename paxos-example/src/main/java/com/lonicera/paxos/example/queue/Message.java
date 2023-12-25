package com.lonicera.paxos.example.queue;

import com.baidu.bjf.remoting.protobuf.annotation.ProtobufClass;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
@ProtobufClass
public class Message {
  private String topic;
  private String value;
}
