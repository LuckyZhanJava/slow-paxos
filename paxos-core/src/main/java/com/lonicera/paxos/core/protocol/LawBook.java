package com.lonicera.paxos.core.protocol;


import com.lonicera.paxos.core.apis.GeneralDecree;
import java.io.IOException;

public interface LawBook {

  long lastDecreeIndex();

  void induce(GeneralDecree decree);

  void close() throws IOException;

}
