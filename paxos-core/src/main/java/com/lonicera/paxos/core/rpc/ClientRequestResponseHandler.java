package com.lonicera.paxos.core.rpc;

import com.lonicera.paxos.core.apis.PaxosApis;
import com.lonicera.paxos.core.codec.ObjectCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.util.HashedWheelTimer;
import io.netty.util.Timeout;
import io.netty.util.TimerTask;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ClientRequestResponseHandler extends ChannelDuplexHandler {

  private static final HashedWheelTimer TIMER = new HashedWheelTimer(new ThreadFactory() {
    private final AtomicLong idGenerator = new AtomicLong();

    @Override
    public Thread newThread(Runnable r) {
      Thread t = new Thread(r);
      t.setName("timeout-thread-" + idGenerator.incrementAndGet());
      t.setDaemon(true);
      return t;
    }
  });

  @AllArgsConstructor
  @Getter
  private static class TimeoutCallback {

    private String requestCode;
    private Timeout timeout;
    private CompletableFuture<ApiResponse> responseFuture;
  }

  private Map<Long, TimeoutCallback> CALLBACK_MAP = new ConcurrentHashMap<>();

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
    if (cause instanceof IOException) {
      log.info("channel : {} error will be close", ctx.channel());
    } else {
      super.exceptionCaught(ctx, cause);
    }
  }

  @Override
  public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise)
      throws Exception {
    if (msg instanceof TimeoutApiRequest) {
      write0(ctx, (TimeoutApiRequest) msg, promise);
    } else {
      super.write(ctx, msg, promise);
    }
  }

  private void write0(ChannelHandlerContext ctx, TimeoutApiRequest timeoutRequest,
      ChannelPromise promise) {

    ApiRequest apiRequest = timeoutRequest.getApiRequest();
    byte[] bytes = ObjectCodec.codec().encode(apiRequest.getBody());
    RpcRequest rpcRequest = new RpcRequest(apiRequest.getId(), apiRequest.getProposerId(),
        apiRequest.getCode(), bytes);
    promise.addListener(new ChannelFutureListener() {
      @Override
      public void operationComplete(ChannelFuture future) throws Exception {
        if (!future.isSuccess()) {
          timeoutRequest.getResponseFuture().completeExceptionally(future.cause());
        } else {
          long rpcId = rpcRequest.getId();
          Timeout timeout = timeout(rpcId, timeoutRequest.getTimeout());
          TimeoutCallback callback = new TimeoutCallback(rpcRequest.getCode(), timeout,
              timeoutRequest.getResponseFuture());
          CALLBACK_MAP.put(rpcId, callback);
        }
      }
    });
    byte[] requestBytes = ObjectCodec.codec().encode(rpcRequest);
    ByteBuf byteBuf = ctx.channel().alloc().buffer();
    byteBuf.writeBytes(requestBytes);

    ctx.write(byteBuf, promise);
  }

  private Timeout timeout(long rpcId, int timeout) {
    Timeout t = TIMER.newTimeout(new TimerTask() {
      @Override
      public void run(Timeout timeout) throws Exception {
        TimeoutCallback callback = CALLBACK_MAP.remove(rpcId);
        if (callback != null) {
          callback.responseFuture.completeExceptionally(new TimeoutException());
        }
      }
    }, timeout, TimeUnit.MILLISECONDS);
    return t;
  }

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
    if (msg instanceof ByteBuf) {
      channelRead0(ctx, (ByteBuf) msg);
    } else {
      super.channelRead(ctx, msg);
    }
  }

  private void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) throws IOException {
    byte[] jsonBytes = ByteBufUtil.getBytes(msg);
    RpcResponse response = ObjectCodec.codec().decode(jsonBytes, RpcResponse.class);
    long rpcId = response.getId();
    TimeoutCallback timeoutCallback = CALLBACK_MAP.remove(rpcId);
    if (timeoutCallback != null) {
      timeoutCallback.getTimeout().cancel();
      Class<?> clazz = PaxosApis.responseType(timeoutCallback.requestCode);
      Object resp = ObjectCodec.codec().decode(response.getBody(), clazz);
      ApiResponse apiResponse = new ApiResponse(response.getId(), response.getErrorCode(), resp);
      timeoutCallback.getResponseFuture().complete(apiResponse);
    }
    ctx.fireChannelRead(msg);
  }
}
