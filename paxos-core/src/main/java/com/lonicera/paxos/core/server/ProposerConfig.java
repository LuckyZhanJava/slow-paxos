package com.lonicera.paxos.core.server;

import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class ProposerConfig {
  private int id;
  private Map<Integer,String> proposerUriConfig;
  private int connectTimeoutMillis;
  private int requestTimeoutMillis;
  private String dataDir;
}
