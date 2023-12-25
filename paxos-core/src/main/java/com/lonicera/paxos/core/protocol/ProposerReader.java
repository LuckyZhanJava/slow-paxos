package com.lonicera.paxos.core.protocol;

import java.io.IOException;

public interface ProposerReader {
  LawBook readLawBook(String dir) throws IOException;
  Ledger readLedger(String dir) throws IOException;;
}
