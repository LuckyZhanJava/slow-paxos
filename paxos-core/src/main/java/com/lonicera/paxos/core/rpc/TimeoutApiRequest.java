package com.lonicera.paxos.core.rpc;

import java.util.concurrent.CompletableFuture;
import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public class TimeoutApiRequest {
  private ApiRequest apiRequest;
  private int timeout;
  private final CompletableFuture<ApiResponse> responseFuture = new CompletableFuture<>();

}
