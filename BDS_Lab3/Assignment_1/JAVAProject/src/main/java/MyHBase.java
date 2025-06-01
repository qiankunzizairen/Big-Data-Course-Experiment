import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;
import java.io.IOException;

public class MyHBase {
    // 自定义业务日志，名称为 "my.own.logger"
    private static final Logger MY_LOGGER = LoggerFactory.getLogger("my.own.logger");
    // HBase API 相关的静态成员
    public static Configuration configuration;
    public static Connection connection;
    public static Admin admin;

    // 初始化 HBase 连接，并设置 Hadoop、HBase、ZK 的日志级别为 ERROR，减少不必要的日志输出
    public static void init() {
        Configurator.setAllLevels("org.apache.hadoop", Level.ERROR);
        Configurator.setAllLevels("org.apache.zookeeper", Level.ERROR);
        Configurator.setAllLevels("org.apache.hadoop.hbase", Level.ERROR);
        Configurator.setAllLevels("my.own.logger", Level.INFO);
        //Configurator.setRootLevel(Level.ERROR);
        configuration = HBaseConfiguration.create();
        configuration.set("hbase.rootdir", "hdfs://localhost:9000/hbase");
        try {
            connection = ConnectionFactory.createConnection(configuration);
            admin = connection.getAdmin();
        } catch (IOException e) {
            MY_LOGGER.error("初始化连接失败！", e);
        }
    }

    // 关闭 HBase 连接和管理接口，释放资源
    public static void close() {
        try {
            if (admin != null) {
                admin.close();
            }
            if (connection != null) {
                connection.close();
            }
        } catch (IOException e) {
            MY_LOGGER.error("关闭连接失败！", e);
        }
    }

    // 创建示例表，利用 HTableDescriptor 描述表结构；TestTable1 包含两个列族 info 和 data，TestTable2 包含列族 cf
    public static void createExampleTables() throws IOException {
        init();
        TableName tableName1 = TableName.valueOf("TestTable1");
        TableName tableName2 = TableName.valueOf("TestTable2");
        HTableDescriptor desc1 = new HTableDescriptor(tableName1);
        desc1.addFamily(new HColumnDescriptor("info"));
        desc1.addFamily(new HColumnDescriptor("data"));
        HTableDescriptor desc2 = new HTableDescriptor(tableName2);
        desc2.addFamily(new HColumnDescriptor("cf"));
        if (!admin.tableExists(tableName1)) {
            admin.createTable(desc1);
            MY_LOGGER.info("表 {} 已创建", tableName1);
        } else {
            MY_LOGGER.info("表 {} 已存在，跳过创建", tableName1);
        }
        if (!admin.tableExists(tableName2)) {
            admin.createTable(desc2);
            MY_LOGGER.info("表 {} 已创建", tableName2);
        } else {
            MY_LOGGER.info("表 {} 已存在，跳过创建", tableName2);
        }
        close();
    }

    // 列出所有表，不包含额外标号，由 main 方法统一输出任务信息
    public static void listTables() throws IOException {
        init();
        HTableDescriptor[] descriptors = admin.listTables();
        for (HTableDescriptor htd : descriptors) {
            MY_LOGGER.info("表名: {}", htd.getNameAsString());
        }
        close();
    }

    // 扫描指定表并输出所有记录数据
    public static void getData(String tableName) throws IOException {
        init();
        Table table = connection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner) {
            printRecord(result);
        }
        scanner.close();
        table.close();
        close();
    }

    // 打印单条记录的详细信息：行键、列族、列、值和时间戳
    private static void printRecord(Result result) {
        for (Cell cell : result.rawCells()) {
            String rowKey = Bytes.toString(CellUtil.cloneRow(cell));
            String cf = Bytes.toString(CellUtil.cloneFamily(cell));
            String col = Bytes.toString(CellUtil.cloneQualifier(cell));
            String val = Bytes.toString(CellUtil.cloneValue(cell));
            long ts = cell.getTimestamp();
            MY_LOGGER.info("行键: {}, 列族: {}, 列: {}, 值: {}, 时间戳: {}", rowKey, cf, col, val, ts);
        }
    }

    // 插入数据到指定表（模拟向表中添加列数据）
    public static void insertRow(String tableName, String rowKey,
                                 String colFamily, String col, String val) throws IOException {
        init();
        Table table = connection.getTable(TableName.valueOf(tableName));
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes(col), Bytes.toBytes(val));
        table.put(put);
        MY_LOGGER.info("向表 {} 的行键 {} 插入列 {}:{} = {}", tableName, rowKey, colFamily, col, val);
        table.close();
        close();
    }

    // 删除指定数据（删除指定行的特定列）
    public static void deleRow(String tableName, String rowKey,
                               String colFamily, String col) throws IOException {
        init();
        Table table = connection.getTable(TableName.valueOf(tableName));
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        delete.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes(col));
        table.delete(delete);
        MY_LOGGER.info("从表 {} 的行键 {} 删除列 {}:{}", tableName, rowKey, colFamily, col);
        table.close();
        close();
    }

    // 统计指定表行数，并打印统计结果
    public static void countRows(String tableName) throws IOException {
        init();
        Table table = connection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        ResultScanner scanner = table.getScanner(scan);
        int count = 0;
        for (Result r = scanner.next(); r != null; r = scanner.next()) {
            count++;
        }
        MY_LOGGER.info("表 {} 的行数: {}", tableName, count);
        scanner.close();
        table.close();
        close();
    }

    // 清空指定表数据：删除表后重新创建（重建时只添加列族 info）
    public static void clearRows(String tableName) throws IOException {
        init();
        TableName tn = TableName.valueOf(tableName);
        if (admin.tableExists(tn)) {
            admin.disableTable(tn);
            admin.deleteTable(tn);
            MY_LOGGER.info("表 {} 已删除", tn);
            HTableDescriptor newDesc = new HTableDescriptor(tn);
            newDesc.addFamily(new HColumnDescriptor("info"));
            admin.createTable(newDesc);
            MY_LOGGER.info("表 {} 已重新创建(空表)", tn);
        } else {
            MY_LOGGER.warn("表 {} 不存在，无法清空", tn);
        }
        close();
    }

    // 删除所有表，清理测试环境
    public static void deleteAllTables() throws IOException {
        init();
        HTableDescriptor[] descriptors = admin.listTables();
        for (HTableDescriptor htd : descriptors) {
            TableName tn = htd.getTableName();
            MY_LOGGER.info("正在删除表: {}", tn);
            admin.disableTable(tn);
            admin.deleteTable(tn);
        }
        close();
    }

    // 主函数：所有主要任务的任务信息统一在此输出，按 (1)-(5) 顺序进行
    public static void main(String[] args) throws IOException {
        // 初始化并创建示例表，插入初始数据
        init();
        createExampleTables();
        insertRow("TestTable1", "row1", "info", "name", "Alice");
        insertRow("TestTable1", "row2", "info", "name", "Bob");
        insertRow("TestTable1", "row3", "info", "name", "Charlie");
        insertRow("TestTable1", "row1", "data", "score", "100");
        insertRow("TestTable1", "row2", "data", "score", "85");
        insertRow("TestTable1", "row3", "data", "score", "90");
        
        MY_LOGGER.info("=== (1) 列出所有表的相关信息 ===");
        listTables();
        
        MY_LOGGER.info("=== (2) 打印指定表 'TestTable1' 的所有记录数据 ===");
        getData("TestTable1");
        
        MY_LOGGER.info("=== (3) 统计表 'TestTable1' 的行数 ===");
        countRows("TestTable1");
        
        MY_LOGGER.info("=== (4) 向表 'TestTable1' 添加/删除指定的列数据 ===");
        // 添加一列：为 row1 添加 info:age 列
        insertRow("TestTable1", "row1", "info", "age", "30");
        // 删除 row2 中 data:score 列
        deleRow("TestTable1", "row2", "data", "score");
        MY_LOGGER.info("=== 打印变更后的数据 ===");
        getData("TestTable1");
        
        MY_LOGGER.info("=== (5) 清空表 'TestTable1' 的所有记录数据 ===");
        clearRows("TestTable1");
        MY_LOGGER.info("=== 打印清空后的数据 ===");
        getData("TestTable1");
        countRows("TestTable1");
        
        // 清理测试环境，删除所有表并再次列出
        deleteAllTables();
        MY_LOGGER.info("测试完毕，所有表已删除。再次列出所有表检查:");
        listTables();
        MY_LOGGER.info("程序执行完毕");
    }
}