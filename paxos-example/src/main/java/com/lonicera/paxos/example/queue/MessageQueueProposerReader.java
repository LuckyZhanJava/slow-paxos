package com.lonicera.paxos.example.queue;

import com.lonicera.paxos.core.protocol.LawBook;
import com.lonicera.paxos.core.protocol.Ledger;
import com.lonicera.paxos.core.protocol.ProposerReader;
import java.io.File;
import java.io.IOException;

public class MessageQueueProposerReader implements ProposerReader {

  private static final String LEDGER_PATH = "ledger/ledger.data";
  private static final String LAW_BOOK_PATH = "lawBook/lawBook.data";

  @Override
  public LawBook readLawBook(String dir) throws IOException {
    File lawBook = new File(dir + "/" + LAW_BOOK_PATH);
    if (!lawBook.exists()) {
      creatFile(lawBook);
    }
    return new MessageQueueLawBook(lawBook);
  }

  @Override
  public Ledger readLedger(String dir) throws IOException {
    File ledger = new File(dir + "/" + LEDGER_PATH);
    if (!ledger.exists()) {
      creatFile(ledger);
    }
    return new MessageQueueLedger(ledger);
  }

  private void creatFile(File file) throws IOException {
    File dirFile = file.getParentFile();
    if (!dirFile.exists()) {
      dirFile.mkdirs();
    }
    file.createNewFile();
  }
}
