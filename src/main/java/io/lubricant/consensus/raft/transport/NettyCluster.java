package io.lubricant.consensus.raft.transport;

import io.lubricant.consensus.raft.RaftResponse;
import io.lubricant.consensus.raft.command.SnapshotArchive.Snapshot;
import io.lubricant.consensus.raft.context.ContextManager;
import io.lubricant.consensus.raft.context.RaftContext;
import io.lubricant.consensus.raft.support.RaftConfig;
import io.lubricant.consensus.raft.support.RaftThreadGroup;
import io.lubricant.consensus.raft.transport.event.*;
import io.lubricant.consensus.raft.transport.rpc.AsyncService;
import io.netty.channel.nio.NioEventLoopGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

@SuppressWarnings("unchecked")
public class NettyCluster implements RaftCluster, EventBus.EventDispatcher {

    private final static Logger logger = LoggerFactory.getLogger(NettyCluster.class);

    private NodeID localID;
    private EventBus eventBus;
    private Map<NodeID, NettyNode> remoteNodes;
    private NioEventLoopGroup eventLoops;
    private NioEventLoopGroup snapEventLoop;
    private ContextManager contextManager;

    public NettyCluster(RaftConfig config) {

        URI localURI = config.localURI();
        NodeID local = new NodeID(localURI.getHost(), localURI.getPort());

        List<URI> remoteURIs = config.remoteURIs();
        List<NodeID> remote = remoteURIs.stream().map(uri -> new NodeID(uri.getHost(), uri.getPort())).collect(Collectors.toList());

        localID = local;
        eventBus = new EventBus(local, this);

        // 创建发送节点线程
        eventLoops = new NioEventLoopGroup(remote.size(), RaftThreadGroup.instance().newFactory("EventNodeGrp-%d"));
        snapEventLoop = new NioEventLoopGroup(1, RaftThreadGroup.instance().newFactory("SnapNodeGrp-%d"));

        remoteNodes = new HashMap<>();
        for (NodeID nodeID : remote) {
            remoteNodes.put(nodeID,
                    new NettyNode(new EventNode(local, nodeID, eventLoops, snapEventLoop)));
        }

        remoteNodes = Collections.unmodifiableMap(remoteNodes);
    }

    public void start(ContextManager contextManager) {
        this.contextManager = contextManager;
        // 作为client连接其他节点
        remoteNodes.forEach((k, v) -> v.connect());
        // 启动自身服务端节点
        eventBus.start();
    }

    @Override
    public void on(PingEvent event) {
        EventID source = event.source();
        NettyNode node = remoteNodes.get(source.nodeID());
        if (node == null) {
            logger.error("Source({}) 没有发现", source);
            return;
        }

        try {
            String contextId = node.parseContextId(source.scope());
            RaftContext context = contextManager.getContext(contextId);
            if (context == null) {
                logger.error("RaftContext({}) of Source({}) 连接没有发现", contextId, source);
                return;
            }
            if (!context.eventLoop().isAvailable()) {
                logger.error("Source({}) 连接不可用", source);
                return;
            }
            // ping事件的时候进行
            Callable invocation = node.prepareLocalInvocation(source.scope(), event.message(), context);
            context.eventLoop().execute(() -> {
                try {
                    // 回应pong事件
                    node.replyRequest(new PongEvent(source, invocation.call(), event.sequence()));
                } catch (Exception e) {
                    logger.error("Source({}) 处理请求失败", source, e);
                }
            });
        } catch (Exception e) {
            logger.error("Source({}) 调度请求失败", source, e);
        }
    }

    @Override
    public void on(PongEvent event) {
        EventID source = event.source();
        NettyNode node = remoteNodes.get(source.nodeID());
        if (node == null) {
            logger.error("Source({}) 该节点不存在", source);
            return;
        }
        AsyncService.Invocation<RaftResponse> invocation =
                node.getInvocationIfPresent(source.scope(), event.sequence());
        if (invocation != null) {
            invocation.accept(event, RaftResponse.class);
        }
    }

    @Override
    public TransSnapEvent on(WaitSnapEvent event) {
        String contextName = event.context();
        try {
            RaftContext context = contextManager.getContext(contextName);
            if (context == null) {
                throw new Exception("找不到上下文");
            }
            Snapshot snapshot = context.snapArchive().lastSnapshot();
            if (snapshot == null) {
                throw new Exception("没有可用的快照");
            }
            if (snapshot.lastIncludeIndex() < event.index() ||
                    snapshot.lastIncludeTerm() < event.term()) {
                throw new Exception("没有符合条件的快照");
            }
            return new TransSnapEvent(snapshot.lastIncludeIndex(), snapshot.lastIncludeTerm(), snapshot.path());
        } catch (Exception e) {
            return new TransSnapEvent(event.index(), event.term(), e.getMessage());
        }
    }

    @Override
    public int size() {
        return remoteNodes.size() + 1;
    }

    @Override
    public NodeID localID() {
        return localID;
    }

    @Override
    public Set<ID> remoteIDs() {
        return (Set) remoteNodes.keySet();
    }

    @Override
    public NettyNode.ServiceStub remoteService(ID nodeID, String ctxID) {
        NettyNode node = remoteNodes.get(nodeID);
        if (node == null) {
            throw new IllegalArgumentException("node not found " + nodeID);
        }
        return node.getService(ctxID);
    }

    @Override
    public void close() throws Exception {
        eventBus.close();
        remoteNodes.forEach((id, node) -> node.close());
    }
}
