/**
 * 用“java.net.URL”和
 * “org.apache.hadoop.fs.FsURLStreamHandlerFactory”
 * 完成输出HDFS中指定文件的文本到终端中。
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;

import java.io.InputStream;
import java.net.URL;

public class MyHDFS {

    // 注册 URL Handler，使 Java 能识别 "hdfs://" 协议
    static {
        try {
            URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
        } catch (Error e) {
            // 如果已经注册过，第二次设置会抛 Error，我们忽略这个异常
            System.err.println("<MyHDFS> URL handler 已注册，跳过重复设置");
        }
    }

    public static void main(String[] args) {
        // 本地文件路径
        String localFilePath = "./text.txt";

        // HDFS 文件路径
        String hdfsFilePath = "/user/hadoop/text.txt";

        try {
            // 配置 Hadoop 环境
            Configuration config = new Configuration();
            config.set("fs.defaultFS", "hdfs://localhost:9000");
            config.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");

            // 加载 Hadoop 配置文件
            config.addResource(new Path("/usr/local/hadoop/etc/hadoop/core-site.xml"));
            config.addResource(new Path("/usr/local/hadoop/etc/hadoop/hdfs-site.xml"));

            // 获取 HDFS 文件系统对象
            FileSystem fs = FileSystem.get(config);

            // 上传本地文件到 HDFS（false 表示不删除本地，true 表示覆盖 HDFS 同名文件）
            Path localPath = new Path(localFilePath);
            Path remotePath = new Path(hdfsFilePath);
            fs.copyFromLocalFile(false, true, localPath, remotePath);
            System.out.println("<MyHDFS> 已将本地文件上传至 HDFS: " + hdfsFilePath);

            // 使用 URL + HDFS 协议打开文件（注意：必须以 hdfs:// 开头）
            InputStream inputStream = new URL("hdfs://localhost:9000" + hdfsFilePath).openStream();
            System.out.println("\n<MyHDFS> 正在读取文件内容：");

            // 将输入流内容复制到标准输出（System.out），缓冲区大小 4096
            IOUtils.copyBytes(inputStream, System.out, 4096, false);

            // 关闭输入流
            inputStream.close();
            System.out.println("\n<MyHDFS> 文件读取完成。");

            // 删除 HDFS 上的该文件，实现环境还原
            if (fs.exists(remotePath)) {
                boolean deleted = fs.delete(remotePath, false);
                if (deleted) {
                    System.out.println("<MyHDFS> 文件已成功删除，实验环境已还原。");
                } else {
                    System.out.println("<MyHDFS> 文件删除失败，请手动检查。");
                }
            }

            // 关闭文件系统
            fs.close();

        } catch (Exception e) {
            // 捕获所有异常并打印
            System.out.println("<MyHDFS> 出现异常: " + e.getMessage());
            e.printStackTrace();
        }
    }
}