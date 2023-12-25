package com.lonicera.paxos.core.protocol;


import com.lonicera.paxos.core.apis.GeneralDecree;
import java.io.IOException;
import java.util.Iterator;

public interface Ledger {

  Iterator<GeneralDecree> decreeItr(long startIndex);

  long lastDecreeIndex();

  long firstDecreeIndex();

  void record(GeneralDecree decree);

  void clear();

  void close() throws IOException;
}
