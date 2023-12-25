package com.lonicera.paxos.core.rpc;


import java.util.concurrent.CompletableFuture;

public interface RpcChannel {
  CompletableFuture<ApiResponse> write(ApiRequest request, int timeoutMillis);
  boolean isActive();
}
