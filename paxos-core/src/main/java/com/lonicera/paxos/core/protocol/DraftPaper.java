package com.lonicera.paxos.core.protocol;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.lonicera.paxos.core.apis.BallotNumber;
import com.lonicera.paxos.core.apis.GeneralDecreeVote;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Data
@NoArgsConstructor
@Slf4j
public class DraftPaper {

  private static final String FILE_NAME = "draft-paper.log";
  private static final int FILE_LENGTH = 1024 * 1024 * 10;
  private int proposerId;
  private int term;//当前任期
  private volatile BallotNumber lastTried; // 发起的最后一个表决
  private GeneralDecreeVote prevGeneralVote; // 最后一个 投票通过的 内容
  private int prevElectTerm; // 最后一个 投票通过的 内容
  private BallotNumber nextBal; // 最后一次 投票通过的 最大编号
  private int logCount;
  private static final int MAX_LOG_COUNT = 20;
  @JsonIgnore
  private File logFile;
  @JsonIgnore
  private FileWriter logFileWriter;
  @JsonIgnore
  private File bakFile;
  @JsonIgnore
  private String dataDir;
  @JsonIgnore
  private String draftDir;

  private static final ObjectMapper objectMapper = new ObjectMapper();

  private DraftPaper(String dataDir, DraftPaperObject draftPaperObject) {
    this.proposerId = draftPaperObject.getProposerId();
    this.term = draftPaperObject.getTerm();
    this.lastTried = draftPaperObject.getLastTried();
    this.prevGeneralVote = draftPaperObject.getPrevGeneralVote();
    this.prevElectTerm = draftPaperObject.getPrevElectTerm();
    this.nextBal = draftPaperObject.getNextBal();
    this.logCount = draftPaperObject.getLogCount();
    this.dataDir = dataDir;
    this.draftDir = dataDir + "/draft/";
    logFile = new File(draftDir + "/" + FILE_NAME);
    if (!logFile.exists()) {
      File parentDir = logFile.getParentFile();
      if (!parentDir.exists()) {
        parentDir.mkdirs();
      }
      try {
        logFile.createNewFile();
      } catch (IOException e) {
        throw new IllegalStateException(e);
      }
    }
    try {
      logFileWriter = new FileWriter(logFile);
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
    bakFile = new File(draftDir + "/" + FILE_NAME + ".bak");
  }

  public static DraftPaper read(String dataDir, int proposerId) throws IOException {
    File dataDirFile = new File(dataDir);
    if (!dataDirFile.exists()) {
      dataDirFile.mkdirs();
    }
    String logFile = dataDir + "/draft/" + FILE_NAME;
    Path logPath = Paths.get(logFile);
    Path bakPath = Paths.get(logFile + ".bak");
    if (!Files.exists(logPath)) {
      if (Files.exists(bakPath)) {
        Files.copy(bakPath, logPath);
      } else {
        return emptyDraftPaper(dataDir, proposerId);
      }
    }
    List<String> logLineList = Files.readAllLines(logPath);
    for (int i = logLineList.size() - 1; i > -1; i--) {
      String log = logLineList.get(i);
      DraftPaperObject paperObject = readJson(log);
      if (paperObject != null) {
        checkDraftProposerId(paperObject.getProposerId(), proposerId);
        DraftPaper paper = new DraftPaper(dataDir, paperObject);
        return paper;
      }
    }
    return emptyDraftPaper(dataDir, proposerId);
  }

  private static DraftPaper emptyDraftPaper(String dataDir, int proposerId) {
    BallotNumber emptyBal = new BallotNumber(0);
    DraftPaperObject paperObject = new DraftPaperObject(proposerId, 0, emptyBal, null, -1, emptyBal,
        0);
    return new DraftPaper(dataDir, paperObject);
  }

  private static void checkDraftProposerId(int storeProposerId, int expectProposerId) {
    if (storeProposerId == expectProposerId) {
      return;
    }
    throw new IllegalStateException(
        "DraftPaper ProposerId : " + storeProposerId + " Not Expect ProposerId : "
            + expectProposerId);
  }

  private static DraftPaperObject readJson(String value) {
    try {
      return objectMapper.readValue(value, DraftPaperObject.class);
    } catch (JsonProcessingException e) {
      log.warn("Read draft paper error",e);
      return null;
    }
  }

  public BallotNumber getLastTried() {
    return lastTried;
  }

  public synchronized boolean setLastTried(BallotNumber newLastTried) {
    if(newLastTried.compareTo(lastTried) > 0){
      this.lastTried = newLastTried;
      persist();
      return true;
    }
    return false;
  }

  public int getTerm() {
    return term;
  }

  public DraftPaper setTerm(int term) {
    this.term = term;
    persist();
    return this;
  }

  public GeneralDecreeVote getPrevGeneralVote() {
    return prevGeneralVote;
  }

  public int getPrevElectTerm() {
    return prevElectTerm;
  }

  public DraftPaper setPrevGeneralVote(GeneralDecreeVote prevGeneralVote) {
    this.prevGeneralVote = prevGeneralVote;
    persist();
    return this;
  }

  public DraftPaper setPrevElectTerm(int prevElectTerm) {
    this.prevElectTerm = prevElectTerm;
    persist();
    return this;
  }

  public BallotNumber getNextBal() {
    return nextBal;
  }

  public synchronized boolean setNextBal(BallotNumber newNextBal) {
    if (newNextBal.compareTo(nextBal) <= 0) {
      return false;
    }
    this.nextBal = newNextBal;
    persist();
    return true;
  }

  private void persist() {
    if (logCount >= MAX_LOG_COUNT) {

      File temp = new File("draft-paper.tmp");
      if (temp.exists()) {
        temp.delete();
      }
      try {
        FileWriter fileWriter = new FileWriter(temp);
        String json = objectMapper.writeValueAsString(newPaperObject(1));
        fileWriter.write(json);
        fileWriter.write("\r\n");
        fileWriter.flush();
        if (bakFile.exists()) {
          bakFile.delete();
        }
        Files.copy(logFile.toPath(), bakFile.toPath());

        logFile.delete();

        logFile = new File(draftDir + "/" + FILE_NAME);
        temp.renameTo(logFile);

        logFileWriter.close();
        logFileWriter = new FileWriter(logFile);
        logCount = 1;
      } catch (IOException e) {
        throw new IllegalStateException(e);
      }
    } else {
      try {
        logCount++;
        String json = objectMapper.writeValueAsString(newPaperObject(logCount));
        logFileWriter.write(json);
        logFileWriter.write("\r\n");
        logFileWriter.flush();
      } catch (JsonProcessingException e) {
        throw new IllegalStateException(e);
      } catch (IOException e) {
        throw new IllegalStateException(e);
      }
    }
  }


  public DraftPaperObject newPaperObject(int logCount) {
    return new DraftPaperObject(proposerId, term, lastTried, prevGeneralVote, prevElectTerm,
        nextBal, logCount);
  }

  public void close() throws IOException {
    logFileWriter.close();
  }
}
