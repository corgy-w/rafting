package io.lubricant.consensus.raft.cluster;

import io.lubricant.consensus.raft.RaftContainer;
import io.lubricant.consensus.raft.cluster.cmd.AppendCommand;
import io.lubricant.consensus.raft.cluster.cmd.FileBasedTestFactory;
import io.lubricant.consensus.raft.command.RaftStub;
import io.lubricant.consensus.raft.support.RaftConfig;
import io.lubricant.consensus.raft.support.RaftException;
import io.lubricant.consensus.raft.support.anomaly.NotLeaderException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;

public class TestNode1 {

    static {
        System.setProperty("LOG_FILE_NAME", "raft1");
    }

    private static final Logger logger = LoggerFactory.getLogger(TestNode1.class);

    public static void main(String[] args) throws Exception {
        RaftContainer container = new RaftContainer(RaftConfig.loadXmlConfig("raft1.xml", true));
        container.create(new FileBasedTestFactory());
        RaftStub root;
        while (true) try {
            root = container.getStub("root");
            if (root != null) break;
            container.openContext("root", true);
            break;
        } catch (RaftException e) {
            logger.warn("创建失败", e);
            Thread.sleep(50000);
        }
        logger.info("创建上下文完成");
        while (true) {
            try {
                int rand = ThreadLocalRandom.current().nextInt(1000);
                // 提交用户指令 追加指令
                root.submit(new AppendCommand("node1-" + rand));
            } catch (Throwable ex) {
                if (! (ex instanceof NotLeaderException)) {
                    logger.info("执行失败: {}", ex.getClass().getSimpleName());
                }
            }
            Thread.sleep(10);
        }
    }

}
