package io.lubricant.consensus.raft;

import org.rocksdb.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class RocksDBTest {
    private static final String dbPath   = "/Users/coolcorgy/data/project/rafting/defaultCF";
    private static final String cfdbPath = "/Users/coolcorgy/data/project/rafting/CertainCF";
    public static void main(String[] args) throws RocksDBException {
        RocksDBTest t=new RocksDBTest();
        t.testDefaultColumnFamily();
        t.testCertainColumnFamily();
    }

    static {
        RocksDB.loadLibrary();
    }


    // RocksDB.DEFAULT_COLUMN_FAMILY 默认列族
    public void testDefaultColumnFamily() throws RocksDBException {
        System.out.println("testDefaultColumnFamily begin...");
        // 文件不存在，则先创建文件
        final Options options = new Options().setCreateIfMissing(true);
        final RocksDB rocksDB = RocksDB.open(options, dbPath);
        // 简单key-value
        byte[] key = "FirstKey".getBytes();
        rocksDB.put(key, "FirstValue".getBytes());
        System.out.println(new String(rocksDB.get(key)));

        rocksDB.put("SecondKey".getBytes(), "SecondValue".getBytes());

        // 通过List做主键查询
        List<byte[]> keys = Arrays.asList(key, "SecondKey".getBytes(), "missKey".getBytes());
        Map<byte[], byte[]> values = rocksDB.multiGet(keys);
        for (int i = 0; i < keys.size(); i++) {
            System.out.println("multiGet " + new String(keys.get(i)) + ":" + (values.get(keys.get(i)) != null ? new String(values.get(keys.get(i))) : null));
        }

        // 打印全部[key - value]
        RocksIterator iter = rocksDB.newIterator();
        for (iter.seekToFirst(); iter.isValid(); iter.next()) {
            System.out.println(new String(iter.key()) + ":" + new String(iter.value()));
        }

        // 删除一个key
        rocksDB.delete(key);
        System.out.println("after remove key:" + new String(key));

        iter = rocksDB.newIterator();
        for (iter.seekToFirst(); iter.isValid(); iter.next()) {
            System.out.println(new String(iter.key()) + ":" + new String(iter.value()));
        }
    }

    // 使用特定的列族打开数据库，可以把列族理解为关系型数据库中的表(table)
    public void testCertainColumnFamily() throws RocksDBException {
        System.out.println("\ntestCertainColumnFamily begin...");
        final ColumnFamilyOptions cfOpts = new ColumnFamilyOptions().optimizeUniversalStyleCompaction();
        String cfName = "my-first-columnfamily";
        // list of column family descriptors, first entry must always be default column family
        final List<ColumnFamilyDescriptor> cfDescriptors = Arrays.asList(
                new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, cfOpts),
                new ColumnFamilyDescriptor(cfName.getBytes(), cfOpts));

        List<ColumnFamilyHandle> cfHandles = new ArrayList<>();
        final DBOptions dbOptions = new DBOptions().setCreateIfMissing(true).setCreateMissingColumnFamilies(true);
        final RocksDB rocksDB = RocksDB.open(dbOptions, cfdbPath, cfDescriptors, cfHandles);
        ColumnFamilyHandle cfHandle = cfHandles.stream().filter(x -> {
            try {
                return (new String(x.getName())).equals(cfName);
            } catch (RocksDBException e) {
                return false;
            }
        }).collect(Collectors.toList()).get(0);

        // 写入key/value
        String key = "FirstKey";
        rocksDB.put(cfHandle, key.getBytes(), "FirstValue".getBytes());
        // 查询单key
        byte[] getValue = rocksDB.get(cfHandle, key.getBytes());
        System.out.println(new String(getValue));

        // 写入第2个key/value
        rocksDB.put(cfHandle, "SecondKey".getBytes(), "SecondValue".getBytes());

        List<byte[]> keys = Arrays.asList(key.getBytes(), "SecondKey".getBytes());
        List<ColumnFamilyHandle> cfHandleList = Arrays.asList(cfHandle, cfHandle);
        // 查询多个key
        Map<byte[], byte[]> values = rocksDB.multiGet(cfHandleList, keys);
        for (int i = 0; i < keys.size(); i++) {
            System.out.println("multiGet:" + new String(keys.get(i)) + ":" + (values.get(keys.get(i)) == null ? null : new String(values.get(keys.get(i)))));
        }

        // 删除单key
        rocksDB.delete(cfHandle, key.getBytes());

        RocksIterator iter = rocksDB.newIterator(cfHandle);
        for (iter.seekToFirst(); iter.isValid(); iter.next()) {
            System.out.println(new String(iter.key()) + ":" + new String(iter.value()));
        }

        for (final ColumnFamilyHandle columnFamilyHandle : cfHandles) {
            columnFamilyHandle.close();
        }
    }
}

