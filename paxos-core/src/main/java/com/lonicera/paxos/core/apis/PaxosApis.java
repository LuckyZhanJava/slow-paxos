package com.lonicera.paxos.core.apis;

import java.util.HashMap;
import java.util.Map;
import lombok.AllArgsConstructor;

public enum PaxosApis {

  NEXT_BALLOT("NEXT_BALLOT", NextBallotRequest.class, LastVoteResponse.class),
  NEXT_ELECT("NEXT_ELECT", NextElectRequest.class, LastElectResponse.class),
  BEGIN_BALLOT("BEGIN_BALLOT", BeginBallotRequest.class, BallotVoteResponse.class),
  SUCCESS_DECREE("SUCCESS_DECREE", RecordDecreeRequest.class, RecordDecreeResponse.class),
  BEGIN_ELECT("BEGIN_ELECT", BeginElectRequest.class, ElectVoteResponse.class),
  SUCCESS_ELECT("SUCCESS_ELECT", SuccessElectRequest.class, SuccessElectResponse.class),
  EXTEND_LEADER_TERM("EXTEND_LEADER_TERM", ExtendLeaderTermRequest.class,
      ExtendLeaderTermResponse.class),
  REPLICATE_DECREES("REPLICATE_DECREES", DecreeReplicateRequest.class,
      DecreeReplicateResponse.class)
  ;

  @AllArgsConstructor
  private static class RequestResponseClass {

    Class<?> requestClass;
    Class<?> responseClass;
  }

  private static Map<String, RequestResponseClass> API_CODE_CLASS_MAP = new HashMap<>();
  private static Map<String, PaxosApis> CODE_API_MAP = new HashMap<>();

  static {
    for (PaxosApis api : PaxosApis.values()) {
      API_CODE_CLASS_MAP
          .put(api.code, new RequestResponseClass(api.requestClass, api.responseClass));
      CODE_API_MAP.put(api.code, api);
    }
  }


  private String code;
  private Class<?> requestClass;
  private Class<?> responseClass;

  PaxosApis(String code, Class<?> requestClass, Class<?> responseClass) {
    this.code = code;
    this.requestClass = requestClass;
    this.responseClass = responseClass;
  }

  public String code() {
    return code;
  }

  public static PaxosApis apiOf(String code) {
    return CODE_API_MAP.get(code);
  }

  public static Class<?> requestType(String code) {
    if (API_CODE_CLASS_MAP.containsKey(code)) {
      return API_CODE_CLASS_MAP.get(code).requestClass;
    }
    return null;
  }

  public static Class<?> responseType(String code) {
    if (API_CODE_CLASS_MAP.containsKey(code)) {
      return API_CODE_CLASS_MAP.get(code).responseClass;
    }
    return null;
  }


}
