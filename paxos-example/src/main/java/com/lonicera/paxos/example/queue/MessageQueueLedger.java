package com.lonicera.paxos.example.queue;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.lonicera.paxos.core.apis.GeneralDecree;
import com.lonicera.paxos.core.protocol.Ledger;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Iterator;
import java.util.List;
import lombok.extern.log4j.Log4j2;

@Log4j2
public class MessageQueueLedger implements Ledger {

  private FileWriter ledgerFileWriter;
  private ObjectMapper objectMapper = new ObjectMapper();

  private List<GeneralDecree> decreeList;
  private GeneralDecree lastDecree;

  public MessageQueueLedger(File ledgerFile) throws IOException {
    this.ledgerFileWriter = new FileWriter(ledgerFile, true);
    List<String> lineList = Files.readAllLines(ledgerFile.toPath());
    decreeList = readDecreeList(lineList);
    lastDecree = decreeList.size() > 0 ? decreeList.get(decreeList.size() - 1) : null;
  }

  private List<GeneralDecree> readDecreeList(List<String> lineList) throws JsonProcessingException {
    List<GeneralDecree> decreeList = Lists.newLinkedList();
    for (String line : lineList) {
      GeneralDecree decree = objectMapper.readValue(line, GeneralDecree.class);
      decreeList.add(decree);
    }
    return decreeList;
  }

  @Override
  public Iterator<GeneralDecree> decreeItr(long startIndex) {
    if(startIndex > decreeList.size() - 1){
      return new Iterator<GeneralDecree>() {
        @Override
        public boolean hasNext() {
          return false;
        }

        @Override
        public GeneralDecree next() {
          return null;
        }
      };
    }
    return decreeList.subList((int) startIndex, decreeList.size()).iterator();
  }

  @Override
  public long lastDecreeIndex() {
    if (lastDecree == null) {
      return -1L;
    }
    return lastDecree.getIndex();
  }

  @Override
  public long firstDecreeIndex() {
    if (decreeList.size() == 0) {
      return -1L;
    }
    return decreeList.get(0).getIndex();
  }

  @Override
  public void record(GeneralDecree decree) {
    try {
      if (decree.getIndex() > decreeList.size()) {
        throw new IllegalStateException("Node ledger not synced with leader");
      }
      if (decree.getIndex() < decreeList.size()) {
        log.info("Decree with index : {} already recorded", decree.getIndex());
      }
      String json = objectMapper.writeValueAsString(decree);
      char[] chars = (json + "\r\n").toCharArray();
      ledgerFileWriter.write(chars);
      ledgerFileWriter.flush();
      decreeList.add(decree);
      lastDecree = decree;
    } catch (IOException e) {
      throw new IllegalArgumentException(e);
    }
  }

  @Override
  public void clear() {

  }

  @Override
  public void close() throws IOException {
    ledgerFileWriter.close();
  }
}
