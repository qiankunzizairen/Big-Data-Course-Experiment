import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.Scanner;

public class MyHBase {
    // 自定义业务日志，名称为 "my.own.logger"
    private static final Logger MY_LOGGER = LoggerFactory.getLogger("my.own.logger");
    // HBase API 相关的静态成员
    public static Configuration configuration;
    public static Connection connection;
    public static Admin admin;

    // 初始化 HBase 连接，并设置相关包的日志级别
    public static void init() {
        // 通过代码动态设置日志级别，减少不必要的输出
        Configurator.setAllLevels("org.apache.hadoop", Level.ERROR);
        Configurator.setAllLevels("org.apache.zookeeper", Level.ERROR);
        Configurator.setAllLevels("org.apache.hadoop.hbase", Level.ERROR);
        Configurator.setAllLevels("my.own.logger", Level.INFO);
        Configurator.setRootLevel(Level.ERROR);

        configuration = HBaseConfiguration.create();
        configuration.set("hbase.rootdir", "hdfs://localhost:9000/hbase");
        try {
            connection = ConnectionFactory.createConnection(configuration);
            admin = connection.getAdmin();
        } catch (IOException e) {
            MY_LOGGER.error("初始化连接失败！", e);
        }
    }

    // 关闭 HBase 连接，释放资源
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

    /* createTable(String tableName, String[] fields)
       创建表，参数 tableName 为表的名称，数组 fields 存储各个列族名称。
       当 HBase 中已存在名为 tableName 的表时，先删除原有表，再创建新的表。
    */
    public static void createTable(String tableName, String[] fields) throws IOException {
        init();
        TableName tName = TableName.valueOf(tableName);
        if (admin.tableExists(tName)) {
            MY_LOGGER.info("表 {} 已存在，先删除再创建", tableName);
            admin.disableTable(tName);
            admin.deleteTable(tName);
        }
        HTableDescriptor descriptor = new HTableDescriptor(tName);
        for (String field : fields) {
            HColumnDescriptor colDescriptor = new HColumnDescriptor(field);
            descriptor.addFamily(colDescriptor);
        }
        admin.createTable(descriptor);
        MY_LOGGER.info("表 {} 已创建", tableName);
        close();
    }

    /* addRecord(String tableName, String row, String[] fields, String[] values)
       向表 tableName 中添加记录；row 为行键，
       fields 数组中每个元素格式为 "列族:列"，values 数组存储对应的值。
    */
    public static void addRecord(String tableName, String row, String[] fields, String[] values) throws IOException {
        init();
        Table table = connection.getTable(TableName.valueOf(tableName));
        for (int i = 0; i < fields.length; i++) {
            Put put = new Put(Bytes.toBytes(row));
            String[] parts = fields[i].split(":");
            put.addColumn(Bytes.toBytes(parts[0]), Bytes.toBytes(parts[1]), Bytes.toBytes(values[i]));
            table.put(put);
            MY_LOGGER.info("向表 {} 的行 {} 插入数据 {}:{} = {}", tableName, row, parts[0], parts[1], values[i]);
        }
        table.close();
        close();
    }

    /* scanColumn(String tableName, String column)
       浏览表 tableName 中某一列或整个列族的数据。
       当 column 中包含冒号，则视为 "列族:列"；否则视为整个列族。
    */
    public static void scanColumn(String tableName, String column) throws IOException {
        init();
        Table table = connection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        if (column.contains(":")) {
            String[] parts = column.split(":");
            scan.addColumn(Bytes.toBytes(parts[0]), Bytes.toBytes(parts[1]));
        } else {
            scan.addFamily(Bytes.toBytes(column));
        }
        ResultScanner scanner = table.getScanner(scan);
        for (Result result = scanner.next(); result != null; result = scanner.next()) {
            showCell(result);
        }
        scanner.close();
        table.close();
        close();
    }

    // 格式化输出扫描结果中的每个单元格
    public static void showCell(Result result) {
        Cell[] cells = result.rawCells();
        for (Cell cell : cells) {
            String rowKey = Bytes.toString(CellUtil.cloneRow(cell));
            String family = Bytes.toString(CellUtil.cloneFamily(cell));
            String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
            String value = Bytes.toString(CellUtil.cloneValue(cell));
            long timestamp = cell.getTimestamp();
            MY_LOGGER.info("行键: {}, 列族: {}, 列: {}, 值: {}, 时间戳: {}",
                    rowKey, family, qualifier, value, timestamp);
        }
    }

    /* modifyData(String tableName, String row, String column, String val)
       修改表 tableName 中指定行 row 在 "列族:列" 下的单元格数据为 val。
    */
    public static void modifyData(String tableName, String row, String column, String val) throws IOException {
        init();
        Table table = connection.getTable(TableName.valueOf(tableName));
        Put put = new Put(Bytes.toBytes(row));
        String[] parts = column.split(":");
        put.addColumn(Bytes.toBytes(parts[0]), Bytes.toBytes(parts[1]), Bytes.toBytes(val));
        table.put(put);
        MY_LOGGER.info("修改表 {} 中行 {} 的 {} 数据为 {}", tableName, row, column, val);
        table.close();
        close();
    }

    /* deleteRow(String tableName, String row)
       删除表 tableName 中指定行 row 的记录。
    */
    public static void deleteRow(String tableName, String row) throws IOException {
        init();
        Table table = connection.getTable(TableName.valueOf(tableName));
        Delete delete = new Delete(Bytes.toBytes(row));
        table.delete(delete);
        MY_LOGGER.info("删除表 {} 中行 {}", tableName, row);
        table.close();
        close();
    }

    // dropTable：删除整张表（用于环境还原）
    public static void dropTable(String tableName) throws IOException {
        init();
        TableName tName = TableName.valueOf(tableName);
        if (admin.tableExists(tName)) {
            admin.disableTable(tName);
            admin.deleteTable(tName);
            MY_LOGGER.info("表 {} 已删除", tableName);
        } else {
            MY_LOGGER.info("表 {} 不存在，无需删除", tableName);
        }
        close();
    }

    // 主函数：依次测试题目中的五个功能，同时加入用户输入逻辑，测试表已存在的情形
    public static void main(String[] args) throws IOException {
        init();
        Scanner scanner = new Scanner(System.in);
        MY_LOGGER.info("程序开始：执行对 [Student, Course, SC] 三张表的测试");

        // (1) 创建三张表：Student、Course、SC（各只定义一个列族 info）
        MY_LOGGER.info("=== (1) 创建 [Student], [Course], [SC] 三张表 ===");
        createTable("Student", new String[]{"info"});
        createTable("Course", new String[]{"info"});
        createTable("SC", new String[]{"info"});
        MY_LOGGER.info("检查：三张表创建完成");

        // 询问用户是否测试表已存在情形
        MY_LOGGER.info("是否测试表已存在的情形？(y/n)：");
        String choice = scanner.nextLine();
        if ("y".equalsIgnoreCase(choice)) {
            MY_LOGGER.info("测试表已存在情形：对 [Student] 表再次调用创建逻辑（将先删除再重建）。");
            createTable("Student", new String[]{"info"});
            MY_LOGGER.info("检查：表 [Student] 已删除并重新创建");
        } else {
            MY_LOGGER.info("不测试表已存在情形。");
        }

        // (2) 插入示例数据：向 Student、Course、SC 表中分别插入数据
        MY_LOGGER.info("=== (2) 向各表中插入示例数据 ===");
        // Student 表数据
        addRecord("Student", "2015001", new String[]{"info:S_Name", "info:S_Sex", "info:S_Age"},
                  new String[]{"Zhangsan", "male", "23"});
        addRecord("Student", "2015002", new String[]{"info:S_Name", "info:S_Sex", "info:S_Age"},
                  new String[]{"Mary", "female", "22"});
        addRecord("Student", "2015003", new String[]{"info:S_Name", "info:S_Sex", "info:S_Age"},
                  new String[]{"Lisi", "male", "24"});
        // Course 表数据
        addRecord("Course", "123001", new String[]{"info:C_Name", "info:C_Credit"},
                  new String[]{"Math", "2.0"});
        addRecord("Course", "123002", new String[]{"info:C_Name", "info:C_Credit"},
                  new String[]{"Computer", "2.5"});
        addRecord("Course", "123003", new String[]{"info:C_Name", "info:C_Credit"},
                  new String[]{"English", "3.0"});
        // SC 表数据（行键格式：学号_课程号）
        addRecord("SC", "2015001_123001", new String[]{"info:SC_Score"}, new String[]{"86"});
        addRecord("SC", "2015001_123003", new String[]{"info:SC_Score"}, new String[]{"69"});
        addRecord("SC", "2015002_123002", new String[]{"info:SC_Score"}, new String[]{"77"});
        addRecord("SC", "2015002_123003", new String[]{"info:SC_Score"}, new String[]{"88"});
        addRecord("SC", "2015003_123002", new String[]{"info:SC_Score"}, new String[]{"90"});
        addRecord("SC", "2015003_123003", new String[]{"info:SC_Score"}, new String[]{"95"});
        MY_LOGGER.info("检查：示例数据插入完成");

        // (3) 浏览数据：扫描 Student 表中 info 列族
        MY_LOGGER.info("=== (3) 浏览 [Student] 表中所有数据（列族 'info'） ===");
        scanColumn("Student", "info");

        // (4) 修改数据：修改 Student 表中 2015001 的 info:S_Age 为 25
        MY_LOGGER.info("=== (4) 修改 [Student] 表中行 '2015001' 的 info:S_Age 修改为 25 ===");
        modifyData("Student", "2015001", "info:S_Age", "25");
        MY_LOGGER.info("检查：修改后扫描 Student 表中 'info:S_Age' 的数据");
        scanColumn("Student", "info:S_Age");

        // (5) 删除数据：删除 Student 表中行 '2015002'
        MY_LOGGER.info("=== (5) 删除 [Student] 表中行 '2015002' 的记录 ===");
        deleteRow("Student", "2015002");
        MY_LOGGER.info("检查：删除后扫描 Student 表中整个 'info' 列族的数据");
        scanColumn("Student", "info");

        // 测试完成后，删除所有插入的表，以还原测试环境
        MY_LOGGER.info("测试完成，删除所有表：[Student], [Course], [SC]");
        dropTable("Student");
        dropTable("Course");
        dropTable("SC");
        MY_LOGGER.info("环境还原完毕，程序结束");

        scanner.close();
    }
}