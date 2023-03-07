package io.lubricant.consensus.raft;

import io.lubricant.consensus.raft.command.RaftStub;
import io.lubricant.consensus.raft.command.admin.Administrator;
import io.lubricant.consensus.raft.command.spi.MachineProvider;
import io.lubricant.consensus.raft.command.spi.StateLoader;
import io.lubricant.consensus.raft.context.ContextManager;
import io.lubricant.consensus.raft.context.RaftContext;
import io.lubricant.consensus.raft.support.RaftConfig;
import io.lubricant.consensus.raft.support.RaftFactory;
import io.lubricant.consensus.raft.transport.RaftCluster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 容器（管理组件的生命周期）
 */
public class RaftContainer {

    private final static Logger logger = LoggerFactory.getLogger(RaftContainer.class);

    private boolean active;
    private RaftConfig config;

    private StateLoader loader;
    private MachineProvider provider;
    private ContextManager manager;
    private RaftCluster cluster;

    private Administrator admin;
    private Map<String, RaftStub> stubMap;

    public RaftContainer(RaftConfig config) {
        // 读取xml文件生成的配置类
        this.config = config;

        // 服务存跟容器
        stubMap = new ConcurrentHashMap<>();

    }

    public synchronized void create(RaftFactory factory) throws Exception {

        logger.info("开始创建 Raft 容器");
        try {
            active = true;

            // 通过配置类加载相应的数据信息
            loader = factory.loadState(config);

            provider = factory.restartMachine(config);

            manager = factory.resumeContext(config);

            cluster = factory.joinCluster(config);

            // 启动netty服务
            factory.bootstrap(cluster, manager, loader, provider);

            //————————————————分水岭——————————————

            // 创建上下文的起始点 获得状态机
            admin = (Administrator) manager.createContext(Administrator.ID).stateMachine();

            // 添加关闭事件钩子
            Runtime.getRuntime().addShutdownHook(new Thread(this::destroy, "RaftContainerHook"));

        } catch (Exception e) {

            logger.info("创建 Raft 容器失败");
            destroy();
            throw e;

        }
        logger.info("创建 Raft 容器成功");
    }

    /**
     * 开启上下文
     *
     * @param contextId 上下文ID
     * @param create    如果不存在则创建
     */
    public boolean openContext(String contextId, boolean create) throws Exception {
        if (!active) {
            throw new IllegalStateException("Container closed");
        }
        if (!Administrator.ID.equals(contextId)) {
            if (admin.get(contextId) != null) return true;
            return admin.open(contextId, create) != null;
        }
        return false;
    }

    /**
     * 关闭上下文
     *
     * @param contextId 上下文ID
     * @param destroy   关闭成功后销毁
     */
    public boolean closeContext(String contextId, boolean destroy) throws Exception {
        if (!active) {
            throw new IllegalStateException("Container closed");
        }
        if (!Administrator.ID.equals(contextId)) {
            if (admin.get(contextId) == null) return true;
            return admin.close(contextId, destroy);
        }
        return false;
    }

    public RaftStub getStub(String contextId) throws Exception {
        if (!active) {
            throw new IllegalStateException("Container closed");
        }
        RaftStub stub = stubMap.get(contextId);
        if (stub != null && stub.refer()) {
            return stub;
        }
        if (active) synchronized (this) {
            if (active) {
                RaftContext context = manager.getContext(contextId);
                if (context == null) {
                    return null;
                }
                stub = new RaftStub(context, stubMap);
                stubMap.put(contextId, stub);
            }
        }
        return stub;
    }

    synchronized void destroy() {
        if (active) {
            active = false;
        } else return;

        logger.info("Start destroying RaftContainer");
        stubMap.clear();

        if (cluster != null) {
            try {
                cluster.close();
            } catch (Exception e) {
                logger.error("Close cluster failed", e);
            }
        }
        if (manager != null) {
            try {
                manager.close();
            } catch (Exception e) {
                logger.error("Close manager failed", e);
            }
        }
        if (provider != null) {
            try {
                provider.close();
            } catch (Exception e) {
                logger.error("Close provider failed", e);
            }
        }
        if (loader != null) {
            try {
                loader.close();
            } catch (Exception e) {
                logger.error("Close loader failed", e);
            }
        }

        admin = null;
        cluster = null;
        manager = null;
        provider = null;
        loader = null;
        logger.info("RaftContainer is destroyed");
    }
}
