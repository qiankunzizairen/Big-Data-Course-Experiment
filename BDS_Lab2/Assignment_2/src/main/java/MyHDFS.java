/**
 * 编程实现一个类，
 * 该类继承“org.apache.hadoop.fs.FSDataInputStream”，
 * 要求如下：实现按行读取HDFS中指定文件的方法“readLine()”，
 * 如果读到文件末尾，则返回空，否则返回文件一行的文本。
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.*;

public class MyHDFS extends FSDataInputStream {

    // 构造函数：继承 FSDataInputStream 必须实现这个构造器
    public MyHDFS(InputStream in) {
        super(in);
    }

    // 自定义逐行读取方法：每次读取一个字符，直到遇到换行符或文件结束
    public static String readLine(BufferedReader reader) throws IOException {
        char[] buffer = new char[1024];  // 存储读取字符的缓冲区
        int bytesRead = -1;
        int offset = 0;

        // 循环读取每个字符，直到遇到 \n 或 EOF
        while ((bytesRead = reader.read(buffer, offset, 1)) != -1) {
            if (String.valueOf(buffer[offset]).equals("\n")) {
                offset += 1;
                break;
            }
            offset += 1;
        }

        // 如果读取到了字符内容，就返回拼接好的字符串
        if (offset > 0) {
            return new String(buffer, 0, offset);
        }

        // 如果没有读取到任何字符，说明文件结束，返回 null
        return null;
    }

    // 读取并打印 HDFS 文件内容（逐行输出）
    public static void displayFileContent(Configuration config, String hdfsFilePath) throws IOException {
        FileSystem fs = FileSystem.get(config);  // 获取 HDFS 文件系统对象
        Path path = new Path(hdfsFilePath);      // 指定读取的文件路径

        // 打开文件输入流并用 BufferedReader 包装以便逐行读取
        FSDataInputStream inputStream = fs.open(path);
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));

        String line;

        // 循环读取每一行，并输出到终端
        while ((line = MyHDFS.readLine(reader)) != null) {
            System.out.println("<MyHDFS> " + line);
        }

        // 关闭流资源
        reader.close();
        inputStream.close();
        fs.close();
    }

    // 上传本地文件到 HDFS（覆盖已存在的文件）
    public static void uploadFileToHDFS(Configuration config, String localPath, String hdfsDestPath) throws IOException {
        FileSystem fs = FileSystem.get(config);
        Path src = new Path(localPath);         // 本地路径
        Path dst = new Path(hdfsDestPath);      // HDFS 目标路径

        // 上传文件（false 表示不是删除本地；true 表示允许覆盖）
        fs.copyFromLocalFile(false, true, src, dst);
        System.out.println("<MyHDFS> 本地文件已上传至 HDFS: " + hdfsDestPath);

        fs.close();
    }

    // 删除 HDFS 上的指定文件
    public static void deleteFileFromHDFS(Configuration config, String hdfsFilePath) throws IOException {
        FileSystem fs = FileSystem.get(config);
        Path path = new Path(hdfsFilePath);

        if (fs.exists(path)) {
            boolean deleted = fs.delete(path, false);  // false 表示非递归删除
            if (deleted) {
                System.out.println("<MyHDFS> 已成功删除 HDFS 文件: " + hdfsFilePath);
            } else {
                System.out.println("<MyHDFS> 删除失败: " + hdfsFilePath);
            }
        } else {
            System.out.println("<MyHDFS> 文件不存在，不需要删除: " + hdfsFilePath);
        }

        fs.close();
    }

    public static void main(String[] args) {

        // 1. 构建 Hadoop 配置对象
        Configuration config = new Configuration();

        // 设置 HDFS 地址和实现类
        config.set("fs.defaultFS", "hdfs://localhost:9000");
        config.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");

        // 加载 core-site.xml 和 hdfs-site.xml 配置
        config.addResource(new Path("/usr/local/hadoop/etc/hadoop/core-site.xml"));
        config.addResource(new Path("/usr/local/hadoop/etc/hadoop/hdfs-site.xml"));

        // 本地文件路径
        String localFile = "./text.txt";

        // HDFS 上传目标路径
        String hdfsFilePath = "/user/hadoop/text.txt";

        try {
            // 上传本地文件到 HDFS
            uploadFileToHDFS(config, localFile, hdfsFilePath);

            // 输出上传的文件内容
            System.out.println("\n===== HDFS 文件内容如下： =====");
            displayFileContent(config, hdfsFilePath);

            // 删除上传的文件，进行环境还原
            System.out.println("\n===== 读取完成，执行环境还原：删除上传的文件 =====");
            deleteFileFromHDFS(config, hdfsFilePath);

        } catch (IOException e) {
            System.out.println("<MyHDFS> 程序出错: " + e.getMessage());
            e.printStackTrace();
        }

        System.out.println("\n<MyHDFS> 程序执行完毕。");
    }
}