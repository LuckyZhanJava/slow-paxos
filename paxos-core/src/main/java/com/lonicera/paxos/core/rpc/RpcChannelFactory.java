package com.lonicera.paxos.core.rpc;


import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.FutureListener;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RpcChannelFactory {

  private int id;
  private String uri;
  private Bootstrap bootstrap;
  private String host;
  private int port;
  private NettyRpcChannel[] rpcChannels;
  private int takeIndex = 0;

  public RpcChannelFactory(
      int id,
      String uri
  ) {
    this.id = id;
    this.uri = uri;
    String[] uriParts = uri.split(":");
    if (uriParts.length != 2) {
      throw new IllegalArgumentException("uri : " + uri);
    }
    this.host = uriParts[0];
    try {
      this.port = Integer.parseInt(uriParts[1]);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("uri : " + uri);
    }
    this.bootstrap = new Bootstrap()
        .channel(NioSocketChannel.class)
        .group(new NioEventLoopGroup(new ThreadFactory() {
          private AtomicLong idGenerator = new AtomicLong();
          @Override
          public Thread newThread(Runnable r) {
            Thread t = new Thread(r);
            t.setDaemon(true);
            t.setName("channel-client-event-loop-" + idGenerator.incrementAndGet());
            return t;
          }
        }))
        .handler(new ChannelInitializer<SocketChannel>() {
          @Override
          protected void initChannel(SocketChannel ch) throws Exception {
            ch.pipeline()
                .addLast(
                    new LengthFieldBasedFrameDecoder(Integer.MAX_VALUE, 0, 4, 0, 4),
                    new LengthFieldPrepender(4),
                    new ClientRequestResponseHandler()
                );
          }
        })
        .remoteAddress(host, port)
    ;
    rpcChannels = new NettyRpcChannel[Runtime.getRuntime().availableProcessors()];
  }

  public RpcChannel channel() {
    synchronized (rpcChannels){
      NettyRpcChannel rpcChannel = rpcChannels[takeIndex];
      takeIndex = takeIndex + 1;
      if(takeIndex >= rpcChannels.length){
        takeIndex = takeIndex % rpcChannels.length;
      }
      if (rpcChannel != null && rpcChannel.isActive()) {
        return rpcChannel;
      }
      ChannelFuture future = bootstrap.connect();
      rpcChannel = new NettyRpcChannel(id, uri, future);
      rpcChannels[takeIndex] = rpcChannel;
      return rpcChannel;
    }
  }

  private static class NettyRpcChannel implements RpcChannel {

    private int id;
    private String uri;
    private Channel nettyChannel;
    private final ChannelFuture future;

    public NettyRpcChannel(int id, String uri, ChannelFuture future) {
      this.id = id;
      this.uri = uri;
      this.future = future;
    }

    @Override
    public boolean isActive() {
      return nettyChannel == null ? true : nettyChannel.isActive();
    }

    @Override
    public CompletableFuture<ApiResponse> write(ApiRequest request, int timeoutMillis) {
      TimeoutApiRequest timeoutApiRequest = new TimeoutApiRequest(
          request,
          timeoutMillis
      );
      if (nettyChannel != null) {
        nettyChannel.writeAndFlush(timeoutApiRequest);
      } else {
        future.addListeners(new FutureListener<Void>() {
          @Override
          public void operationComplete(Future<Void> f) throws Exception {
            if (f.isSuccess()) {
              Channel channel = future.channel();
              nettyChannel = channel;
              channel.writeAndFlush(timeoutApiRequest);
            }else{
              log.error("Write request to Node : {} error : {}", id, f.cause());
              nettyChannel = future.channel();
              nettyChannel.close();
            }
          }
        });
      }

      CompletableFuture<ApiResponse> responseFuture = timeoutApiRequest.getResponseFuture();

      return timeoutApiRequest.getResponseFuture();
    }
  }
}
