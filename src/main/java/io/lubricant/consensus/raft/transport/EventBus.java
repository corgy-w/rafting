package io.lubricant.consensus.raft.transport;

import io.lubricant.consensus.raft.support.RaftThreadGroup;
import io.lubricant.consensus.raft.transport.event.*;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;
import io.netty.util.concurrent.GenericFutureListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;

/**
 * 事件总线（接收其他节点发送的事件） netty服务端代码
 */
public class EventBus extends Thread {

    private final static Logger logger = LoggerFactory.getLogger(EventBus.class);

    public interface EventDispatcher {
        void on(PingEvent event);

        void on(PongEvent event);

        TransSnapEvent on(WaitSnapEvent event);
    }

    private final AttributeKey<NodeID> CHANNEL_ID =
            AttributeKey.newInstance("channelID");

    private final AttributeKey<String> CHANNEL_NAME =
            AttributeKey.newInstance("channelName");

    private final NodeID local;
    private final EventDispatcher dispatcher; //调度
    private final ConcurrentHashMap<NodeID, Channel> nodes;
    private Channel channel;

    public EventBus(NodeID id, EventDispatcher dispatcher) {
        super("EventBus-" + id.port());
        this.local = id;
        this.dispatcher = dispatcher;
        this.nodes = new ConcurrentHashMap<>();
    }

    /**
     * 断开链接处理handler
     */
    private class DisconnectedHandler extends ChannelInboundHandlerAdapter {

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            Attribute<String> name = ctx.channel().attr(CHANNEL_NAME);
            logger.error("Uncaught exception in channel {}", name.get(), cause);
            ctx.close();
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) throws Exception {
            if (ctx.channel().hasAttr(CHANNEL_ID)) {
                NodeID nodeID = ctx.channel().attr(CHANNEL_ID).get();
                if (nodes.compute(nodeID, (id, ch) -> ch == ctx.channel() ? null : ch) != null) {
                    logger.warn("Node({}) channel not found", nodeID);
                }
            }
            Attribute<String> name = ctx.channel().attr(CHANNEL_NAME);
            logger.info("Node({}) channel closed", name.get());
        }
    }

    /**
     * 第一次连接处理handler
     */
    private class FirstConnectHandler extends ChannelInboundHandlerAdapter {

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) {
            Attribute<String> name = ctx.channel().attr(CHANNEL_NAME);
            if (msg instanceof ShakeHandEvent) {
                ShakeHandEvent event = (ShakeHandEvent) msg;
                try {
                    String message = event.message();
                    name.set(message);
                    NodeID nodeID = NodeID.fromString(message);
                    if (local.equals(nodeID) || ctx.channel() != nodes.compute(nodeID,
                            (id, ch) -> ch == null || !ch.isActive() ? ctx.channel() : ch)) {
                        throw new IllegalStateException("重复连接 " + nodeID);
                    }
                    ctx.channel().attr(CHANNEL_ID).set(nodeID);
                    ctx.pipeline().replace(
                            EventCodec.STR_EVENT_DECODER,
                            EventCodec.BIN_EVENT_DECODER,
                            EventCodec.binEventDecoder(nodeID));
                    ctx.pipeline()
                            .addLast(new EventDispatchHandler(nodeID));
                    ctx.writeAndFlush(ShakeHandEvent.ok());
                } catch (Exception e) {
                    logger.error("Node({}) 握手失败", event.message(), e);
                    ctx.writeAndFlush(new ShakeHandEvent(e.getMessage()));
                    ctx.close();
                }
            } else if (msg instanceof WaitSnapEvent) {
                WaitSnapEvent event = (WaitSnapEvent) msg;
                TransSnapEvent snap = dispatcher.on(event);
                name.set(event.context());
                ctx.writeAndFlush(snap);
                if (snap.length() == -1L) {
                    ctx.close();
                } else {
                    DefaultFileRegion region = new DefaultFileRegion(snap.file().getChannel(), 0L, snap.length());
                    ctx.writeAndFlush(region).addListener(future -> {
                        ctx.close();
                        snap.file().close();
                    });
                }
            } else {
                ctx.fireChannelRead(msg);
            }
        }

    }

    private class EventDispatchHandler extends ChannelInboundHandlerAdapter {

        private final NodeID nodeID;

        EventDispatchHandler(NodeID nodeID) {
            this.nodeID = nodeID;
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
            if (msg instanceof Event.BinEvent) {
                EventID src = ((Event.BinEvent) msg).source();
                if (src.nodeID().equals(nodeID)) {
                    if (msg instanceof PingEvent) {

                        dispatcher.on((PingEvent) msg);
                    } else if (msg instanceof PongEvent) {

                        dispatcher.on((PongEvent) msg);
                    } else {
                        logger.error("Unexpected event {}", msg);
                    }
                } else {
                    throw new IllegalArgumentException(
                            String.format("nodeID mismatch: %s != %s", src.nodeID(), nodeID));
                }
            } else {
                ctx.fireChannelRead(msg);
            }
        }
    }

    @Override
    public void run() {
        EventLoopGroup bossGroup = new NioEventLoopGroup(1, RaftThreadGroup.instance().newFactory("EventSrvGrp-%d"));
        EventLoopGroup workerGroup = new NioEventLoopGroup(0, RaftThreadGroup.instance().newFactory("EventBusGrp-%d"));
        try {
            ServerBootstrap b = new ServerBootstrap();
            b.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        public void initChannel(SocketChannel ch) throws Exception {
                            ch.pipeline()
                                    .addLast(EventCodec.frameDecoder())
                                    .addLast(EventCodec.STR_EVENT_DECODER, EventCodec.strEventDecoder())
                                    .addLast(EventCodec.frameEncoder())
                                    .addLast(EventCodec.eventEncoder())
                                    .addLast(new FirstConnectHandler())
                                    .addLast(new DisconnectedHandler());
                        }
                    })
                    .option(ChannelOption.SO_BACKLOG, 128)
                    .childOption(ChannelOption.SO_KEEPALIVE, true);
            ChannelFuture f = b.bind(local.port()).addListener(new GenericFutureListener<ChannelFuture>() {
                @Override
                public void operationComplete(ChannelFuture future) throws Exception {
                    logger.info("EventBus启动成功");
                }
            }).sync();
            channel = f.channel();
            channel.closeFuture().sync();
        } catch (InterruptedException ignored) {
        } finally {
            workerGroup.shutdownGracefully();
            bossGroup.shutdownGracefully();
        }
    }

    public void close() {
        if (channel != null) {
            channel.close();
        }
    }

}
