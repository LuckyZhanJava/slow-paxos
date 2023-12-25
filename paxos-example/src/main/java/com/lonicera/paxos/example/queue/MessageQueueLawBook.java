package com.lonicera.paxos.example.queue;

import com.lonicera.paxos.core.apis.GeneralDecree;
import com.lonicera.paxos.core.protocol.LawBook;
import java.io.File;
import java.io.IOException;

public class MessageQueueLawBook implements LawBook {


  public MessageQueueLawBook(File bookFile) {

  }


  @Override
  public long lastDecreeIndex() {
    return 0;
  }

  @Override
  public void induce(GeneralDecree decree) {

  }

  @Override
  public void close() throws IOException {

  }
}
