package com.lonicera.paxos.core.server;


import com.lonicera.paxos.core.protocol.Proposer;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;

public class ClusterRpcServer {

  private String uri;
  private ServerBootstrap serverBootstrap;
  private NioEventLoopGroup eventLoopGroup;

  public ClusterRpcServer(Proposer proposer, String uri) {
    this.uri = uri;
    eventLoopGroup = new NioEventLoopGroup();
    serverBootstrap = new ServerBootstrap()
        .channel(NioServerSocketChannel.class)
        .group(eventLoopGroup)
        .childHandler(new ChannelInitializer<SocketChannel>() {
          @Override
          protected void initChannel(SocketChannel ch) throws Exception {
            ch.pipeline()
                .addLast(
                    new LengthFieldBasedFrameDecoder(Integer.MAX_VALUE, 0, 4, 0 ,4),
                    new LengthFieldPrepender(4),
                    new ClusterApiRequestProcessHandler(proposer)
                );
          }
        });
  }

  public void start() throws Exception {
    String[] uriParts = uri.split(":");
    serverBootstrap.bind(uriParts[0], Integer.valueOf(uriParts[1])).get();
  }

  public void shutdown() {
    try {
      eventLoopGroup.shutdownGracefully().sync();
    } catch (InterruptedException e) {
      throw new IllegalStateException();
    }
  }
}
