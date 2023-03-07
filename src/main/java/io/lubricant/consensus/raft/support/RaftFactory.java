package io.lubricant.consensus.raft.support;


import io.lubricant.consensus.raft.command.spi.MachineProvider;
import io.lubricant.consensus.raft.command.spi.StateLoader;
import io.lubricant.consensus.raft.command.storage.RocksStateLoader;
import io.lubricant.consensus.raft.context.ContextManager;
import io.lubricant.consensus.raft.transport.NettyCluster;
import io.lubricant.consensus.raft.transport.RaftCluster;

/**
 * 全局工厂（产生容器所依赖的所有组件） Raft生产工厂
 */
public abstract class RaftFactory {

    public StateLoader loadState(RaftConfig config) throws Exception {
        return new RocksStateLoader(config);
    }

    public ContextManager resumeContext(RaftConfig config) throws Exception {
        return new ContextManager(config);
    }

    public RaftCluster joinCluster(RaftConfig config) throws Exception {
        return new NettyCluster(config);
    }

    /**
     * netty线程启动类
     */
    public void bootstrap(RaftCluster cluster, ContextManager manager,
                          StateLoader loader, MachineProvider provider) throws Exception {
        // netty服务启动准备
        manager.start(cluster, loader, provider);
        // 启动服务端
        ((NettyCluster) cluster).start(manager);
    }

    public abstract MachineProvider restartMachine(RaftConfig config) throws Exception;

}
