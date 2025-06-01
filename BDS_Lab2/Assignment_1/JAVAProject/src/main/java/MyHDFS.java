  /**
   * 实验2任务1的11个功能点被我整合为下面的流程：
    startup：在HDFS上创建Lab2文件夹，Lab2文件夹内创建testdir文件夹，在testdir中创建test.txt空文件，并写入“lab2 test”（6、7），
    向HDFS再次上传同名文件test.txt，追加、覆盖支持（1），追加支持开头或结尾（8）；
    输出追加后的文件到终端（3）
    输出Lab2文件夹及内部所有文件的信息（递归支持）（4、5）
    将test.txt移动到Lab2文件夹中（11）
    从HDFS中下载test.txt（重命名副本支持）（2）
    teardown：清理环境，删除Lab2文件夹及其内部测试文件和目录（6、7、9、10）
 */


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import java.io.*;
import java.util.Scanner;

public class MyHDFS {

    /**
     * 判断HDFS路径是否存在
     */
    public static boolean checkPathExistence(Configuration config, String path) throws IOException {
        FileSystem fs = FileSystem.get(config);
        boolean exists = fs.exists(new Path(path));
        fs.close();
        return exists;
    }

    /**
     * 创建HDFS目录
     */
    public static void createDirectory(Configuration config, String dirPath) throws IOException {
        FileSystem fs = FileSystem.get(config);
        Path dir = new Path(dirPath);
        if (!fs.exists(dir)) {
            fs.mkdirs(dir);
            System.out.println("<MyHDFS> 目录已创建: " + dirPath);
        } else {
            System.out.println("<MyHDFS> 目录已存在: " + dirPath);
        }
        fs.close();
    }

    /**
     * 在HDFS上创建文件并写入指定内容（若文件已存在则覆盖）
     */
    public static void createFileWithContent(Configuration config, String filePath, String content) throws IOException {
        FileSystem fs = FileSystem.get(config);
        Path path = new Path(filePath);
        // 使用 create() 会覆盖已存在的文件
        try (FSDataOutputStream outputStream = fs.create(path, true)) {
            outputStream.write(content.getBytes());
        }
        System.out.println("<MyHDFS> 文件创建并写入成功: " + filePath);
        fs.close();
    }

    /**
     * 将本地文件上传到HDFS指定路径（覆盖）
     */
    public static void uploadFileToHDFS(Configuration config, String localFilePath, String remotePath) throws IOException {
        FileSystem fs = FileSystem.get(config);
        Path localPath = new Path(localFilePath);
        Path hdfsDestination = new Path(remotePath);
        // copyFromLocalFile(false, true, ...) 会覆盖同名文件
        fs.copyFromLocalFile(false, true, localPath, hdfsDestination);
        System.out.println("<MyHDFS> 已将本地文件上传(覆盖)到HDFS: " + localFilePath + " -> " + remotePath);
        fs.close();
    }

    /**
     * 读取HDFS文件内容并返回
     */
    public static String readFileContent(Configuration config, String hdfsFilePath) throws IOException {
        FileSystem fs = FileSystem.get(config);
        Path path = new Path(hdfsFilePath);
        if (!fs.exists(path)) {
            fs.close();
            throw new FileNotFoundException("<MyHDFS> 文件不存在: " + hdfsFilePath);
        }

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (FSDataInputStream fis = fs.open(path)) {
            byte[] buffer = new byte[4096];
            int bytesRead;
            while ((bytesRead = fis.read(buffer)) != -1) {
                baos.write(buffer, 0, bytesRead);
            }
        }
        fs.close();
        return baos.toString();
    }

    /**
     * 追加内容到HDFS文件的末尾
     * 若目标文件系统不支持append，需要在Hadoop配置中启用append相关功能
     */
    public static void appendContentToEnd(Configuration config, String hdfsFilePath, String content) throws IOException {
        FileSystem fs = FileSystem.get(config);
        Path path = new Path(hdfsFilePath);

        // 如果文件不存在，需要先创建
        if (!fs.exists(path)) {
            createFileWithContent(config, hdfsFilePath, content);
            return;
        }

        // 如果当前文件系统不支持append，可以用“先读后写”的方式模拟
        // 这里假设可以直接append，如果不行请改用 read + create 覆盖写回
        try (FSDataOutputStream fos = fs.append(path)) {
            fos.write(content.getBytes());
        } catch (IOException e) {
            // 如果 append 不支持，就使用模拟方式
            fs.close();
            simulateAppend(config, hdfsFilePath, content, false);
            return;
        }

        System.out.println("<MyHDFS> 已向文件末尾追加内容: " + hdfsFilePath);
        fs.close();
    }

    /**
     * 追加内容到HDFS文件的开头（HDFS无直接prepend，需模拟: 先读出原内容，再覆盖写“新内容+原内容”）
     */
    public static void appendContentToBeginning(Configuration config, String hdfsFilePath, String content) throws IOException {
        FileSystem fs = FileSystem.get(config);
        Path path = new Path(hdfsFilePath);

        // 如果文件不存在，直接创建写入即可
        if (!fs.exists(path)) {
            createFileWithContent(config, hdfsFilePath, content);
            return;
        }
        fs.close();

        // 使用“先读后写”模拟prepend
        simulateAppend(config, hdfsFilePath, content, true);
    }

    /**
     * 用“读取原内容 + 覆盖写”的方式，模拟向开头或末尾追加
     * @param prepend 为 true 表示将新内容放在前面，否则追加在后面
     */
    private static void simulateAppend(Configuration config, String hdfsFilePath, String newContent, boolean prepend) throws IOException {
        // 1. 读取原文件内容
        String oldContent = "";
        try {
            oldContent = readFileContent(config, hdfsFilePath);
        } catch (FileNotFoundException e) {
            // 若文件不存在则忽略
        }

        // 2. 根据 prepend 或者 append 的标记重组内容
        String finalContent;
        if (prepend) {
            finalContent = newContent + oldContent;
        } else {
            finalContent = oldContent + newContent;
        }

        // 3. 覆盖写回
        createFileWithContent(config, hdfsFilePath, finalContent);
        System.out.println("<MyHDFS> 使用模拟方式" + (prepend ? "在开头" : "在末尾") + "追加内容成功: " + hdfsFilePath);
    }

    /**
     * 递归列出指定目录下的所有文件和子目录
     */
    public static void listFilesRecursively(Configuration config, String directoryPath) throws IOException {
        FileSystem fs = FileSystem.get(config);
        Path path = new Path(directoryPath);

        if (!fs.exists(path)) {
            System.out.println("<MyHDFS> 目录不存在: " + directoryPath);
            fs.close();
            return;
        }

        System.out.println("<MyHDFS> 递归列出目录: " + directoryPath);
        listFilesHelper(fs, path, 0);
        fs.close();
    }

    private static void listFilesHelper(FileSystem fs, Path path, int level) throws IOException {
        FileStatus[] fileStatuses = fs.listStatus(path);
        for (FileStatus status : fileStatuses) {
            String indent = new String(new char[level]).replace("\0", "   ");
            if (status.isDirectory()) {
                System.out.println(indent + "[DIR ] " + status.getPath().getName());
                listFilesHelper(fs, status.getPath(), level + 1);
            } else {
                System.out.println(indent + "[FILE] " + status.getPath().getName() + " (size=" + status.getLen() + ")");
            }
        }
    }

    /**
     * 重命名或移动文件/目录
     */
    public static void renamePath(Configuration config, String srcPath, String dstPath) throws IOException {
        FileSystem fs = FileSystem.get(config);
        Path src = new Path(srcPath);
        Path dst = new Path(dstPath);

        if (!fs.exists(src)) {
            System.out.println("<MyHDFS> 源路径不存在: " + srcPath);
            fs.close();
            return;
        }
        // 目标路径父目录如果不存在，需先创建
        fs.mkdirs(dst.getParent());

        boolean success = fs.rename(src, dst);
        if (success) {
            System.out.println("<MyHDFS> 重命名/移动成功: " + srcPath + " -> " + dstPath);
        } else {
            System.out.println("<MyHDFS> 重命名/移动失败: " + srcPath + " -> " + dstPath);
        }
        fs.close();
    }

    /**
     * 删除HDFS上的文件或目录
     * @param recursive 若要删除目录及其所有内容，应为 true
     */
    public static void deletePath(Configuration config, String path, boolean recursive) throws IOException {
        FileSystem fs = FileSystem.get(config);
        Path p = new Path(path);

        if (!fs.exists(p)) {
            System.out.println("<MyHDFS> 要删除的路径不存在: " + path);
            fs.close();
            return;
        }

        boolean result = fs.delete(p, recursive);
        if (result) {
            System.out.println("<MyHDFS> 删除成功: " + path);
        } else {
            System.out.println("<MyHDFS> 删除失败: " + path);
        }
        fs.close();
    }

    /**
     * 从HDFS下载文件到本地。如果本地存在同名文件，则对下载的文件进行自动重命名
     */
    public static void downloadFileFromHDFS(Configuration config, String hdfsFilePath, String localDir) throws IOException {
        FileSystem fs = FileSystem.get(config);
        Path src = new Path(hdfsFilePath);

        // 如果HDFS文件不存在，直接提示后返回
        if (!fs.exists(src)) {
            System.out.println("<MyHDFS> HDFS文件不存在: " + hdfsFilePath);
            fs.close();
            return;
        }

        // 确保本地目录存在
        File localDirectory = new File(localDir);
        if (!localDirectory.exists()) {
            localDirectory.mkdirs();
        }

        // 构造要在本地保存的路径
        String fileName = src.getName(); 
        File localFile = new File(localDirectory, fileName);

        // 如果本地已存在同名文件，则对下载文件进行自动改名
        if (localFile.exists()) {
            // 例如给它加上一个时间戳前缀/后缀
            String renamedFileName = "download_" + System.currentTimeMillis() + "_" + fileName;
            File renamedFile = new File(localDirectory, renamedFileName);
            fs.copyToLocalFile(false, src, new Path(renamedFile.getAbsolutePath()));
            System.out.println("<MyHDFS> 本地已有同名文件，已将下载文件命名为: " + renamedFileName);
        } else {
            // 不存在同名文件时，按原名直接下载
            fs.copyToLocalFile(false, src, new Path(localFile.getAbsolutePath()));
            System.out.println("<MyHDFS> 下载完成: " + localFile.getAbsolutePath());
        }

        fs.close();
    }

    public static void main(String[] args) {
        
        Configuration config = null; 
        try {
            // 1. 配置
            config = new Configuration();
            // 根据自己实际情况设置
            config.set("fs.defaultFS", "hdfs://localhost:9000");
            config.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
            // 加载 core-site.xml 和 hdfs-site.xml
            config.addResource(new Path("/usr/local/hadoop/etc/hadoop/core-site.xml"));
            config.addResource(new Path("/usr/local/hadoop/etc/hadoop/hdfs-site.xml"));
            
            FileSystem fs = FileSystem.get(config);
         } catch (IOException e) {
            e.printStackTrace();
        }

        // 要操作的路径示例
        String lab2Dir      = "/Lab2";
        String testdirDir   = "/Lab2/testdir";
        String testFilePath = "/Lab2/testdir/test.txt"; // 最初创建
        String localFile    = "./test.txt";

        Scanner scanner = new Scanner(System.in);

        try {
            System.out.println("\n===== startup 阶段 =====");
            // (1) 创建 /Lab2 文件夹
            createDirectory(config, lab2Dir);
            // (2) 在 /Lab2 内创建 testdir 文件夹
            createDirectory(config, testdirDir);
            // (3) 在 testdir 中创建 test.txt 空文件并写入 “lab2 test”
            createFileWithContent(config, testFilePath, "lab2 test\n");

            // 输出文件到终端
            System.out.println("\n===== 输出 " + testFilePath + " 文件内容 =====");
            try {
                String content = readFileContent(config, testFilePath);
                System.out.println("<MYHDFS> 文件内容如下：\n" + content);
            } catch (Exception e) {
                System.out.println("<MYHDFS> 读取文件内容出错: " + e.getMessage());
            }

            // ===== 再次上传同名文件 =====
            System.out.println("\n<MYHDFS> 现在需要再次上传同名文件 test.txt");
            System.out.println("<MYHDFS> 请选择操作方式：");
            System.out.println("<MYHDFS> 1. 追加 (append)");
            System.out.println("<MYHDFS> 2. 覆盖 (overwrite)");
            System.out.print("<MYHDFS> 输入您的选择(1 或 2): ");
            String choice = scanner.nextLine();

            if ("1".equals(choice)) {
                // 追加
                System.out.println("<MYHDFS> 追加内容到文件时，您想要追加到【开头】还是【末尾】？");
                System.out.println("<MYHDFS> a. 开头");
                System.out.println("<MYHDFS> b. 末尾");
                System.out.print("<MYHDFS> 选择(a 或 b): ");
                String ab = scanner.nextLine();

                // 先把本地文件 test.txt 内容读取为字符串
                String localContent = readLocalFile(localFile);

                if ("a".equalsIgnoreCase(ab)) {
                    appendContentToBeginning(config, testFilePath, localContent);
                } else {
                    appendContentToEnd(config, testFilePath, localContent);
                }
            } else if ("2".equals(choice)) {
                // 覆盖
                uploadFileToHDFS(config, localFile, testFilePath);
            } else {
                System.out.println("<MYHDFS> 无效选择，跳过上传。");
            }

            // 输出追加或覆盖后的文件到终端
            System.out.println("\n===== 输出 " + testFilePath + " 文件内容 =====");
            try {
                String content = readFileContent(config, testFilePath);
                System.out.println("<MYHDFS> 文件内容如下：\n" + content);
            } catch (Exception e) {
                System.out.println("<MYHDFS> 读取文件内容出错: " + e.getMessage());
            }

            // 递归列出Lab2文件夹及其内部信息
            System.out.println("\n===== 递归列出 " + lab2Dir + " 内部信息 =====");
            listFilesRecursively(config, lab2Dir);

            // 将 test.txt 从 testdir 移动到 Lab2 文件夹中
            System.out.println("\n===== 移动 test.txt 到 /Lab2 中 =====");
            String destFilePath = "/Lab2/test.txt";
            renamePath(config, testFilePath, destFilePath);

            // 递归列出Lab2文件夹及其内部信息
            System.out.println("\n===== 再次，递归列出 " + lab2Dir + " 来验证 =====");
            listFilesRecursively(config, lab2Dir);

            // 从HDFS中下载文件
            System.out.println("\n===== 从HDFS下载指定文件，若本地同名则自动改名 =====");
            System.out.println("<MYHDFS> 请输入要下载的 HDFS 文件(例如 /Lab2/test.txt 或 /Lab2/test_bak.txt):");
            String hdfsDownloadPath = scanner.nextLine();
            if (hdfsDownloadPath == null || hdfsDownloadPath.trim().isEmpty()) {
                // 若用户没输入，就用 /Lab2/test.txt 做示例
                hdfsDownloadPath = destFilePath;
            }

            // 指定你要下载到的本地目录
            String localDownloadDir = "./";
            downloadFileFromHDFS(config, hdfsDownloadPath, localDownloadDir);

            // teardown：清理环境，删除 Lab2 文件夹及其内部所有内容
            System.out.println("\n===== teardown 阶段，是否删除整个 /Lab2 目录？ =====");
            System.out.println("<MYHDFS> y. 删除");
            System.out.println("<MYHDFS> 其他. 保留");
            System.out.print("<MYHDFS> 请输入选择: ");
            String teardownChoice = scanner.nextLine();
            if ("y".equalsIgnoreCase(teardownChoice)) {
                deletePath(config, lab2Dir, true);
            } else {
                System.out.println("<MYHDFS> 保留 /Lab2 目录, 结束");
            }

            System.out.println("\n<MyHDFS> 所有操作结束。");
        } catch (Exception e) {
            System.out.println("<MyHDFS> 出现错误: " + e.getMessage());
            e.printStackTrace();
        } finally {
            scanner.close();
        }
    }

    /**
     * 读取本地文件内容为字符串
     */
    private static String readLocalFile(String localFilePath) {
        StringBuilder sb = new StringBuilder();
        try (BufferedReader br = new BufferedReader(new FileReader(localFilePath))) {
            String line;
            while ((line = br.readLine()) != null) {
                sb.append(line).append("\n");
            }
        } catch (IOException e) {
            System.out.println("<MyHDFS> 读取本地文件出错: " + e.getMessage());
        }
        return sb.toString();
    }
}