package io.lubricant.consensus.raft.support.serial;


import io.lubricant.consensus.raft.command.RaftStub.Command;
import io.lubricant.consensus.raft.command.RaftLog.Entry;
import io.lubricant.consensus.raft.support.anomaly.SerializeException;

/**
 * 命令序列化器
 */
public interface CmdSerializer {

    /**
     * 将日志转换为命令，供状态机执行 反序列化
     */
    Command deserialize(Entry entry) throws SerializeException;

    default byte[] serialize(Command cmd) throws SerializeException {
        return Serialization.writeObject(cmd);
    }

    /**
     * 序列化
     */
    default byte[] serialize(byte[] meta, Command cmd) throws SerializeException {
        return Serialization.writeObject(meta, cmd);
    }

}
