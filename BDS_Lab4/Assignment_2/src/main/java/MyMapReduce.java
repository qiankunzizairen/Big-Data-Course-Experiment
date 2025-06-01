//主要思想：
/**
 * 	Mapper：
 *  输入： key 是行的偏移量，value 是文本行
	处理： 每行数据使用 IntWritable 类型进行包装。
	输出： 输出格式是 <整数, 1>。
 */
/**
 * Reduce：
 * 输入： <整数, 1>
   处理： 只关注 key，按顺序输出排序位次。
   输出： <排序位次, 数字本身>
 */
import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MyMapReduce {

    // Map 类：负责读取输入文件的每一行
    // 每一行代表一个整数，作为 key 输出，value 固定为 1（用于计数）
    public static class Map extends Mapper<Object, Text, IntWritable, IntWritable> {
        private static final IntWritable data = new IntWritable();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // 将读取到的一行文本转为整数
            String line = value.toString();
            data.set(Integer.parseInt(line));
            // 输出格式为 <整数, 1>，value 为 1 表示出现一次
            context.write(data, new IntWritable(1));
        }
    }

    // Reduce 类：对 Map 阶段输出的 key（整数）排序后，计算它们的排序位次
    public static class Reduce extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
        private int lineNum = 1; // 行号，从 1 开始，表示排序位次

        @Override
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            // 每个 key 可能有多个 value（代表出现多次）
            for (IntWritable val : values) {
                // 输出格式为：<排序位次, 数字本身>
                context.write(new IntWritable(lineNum), key);
                lineNum++;
            }
        }
    }

    // Partition 类：自定义分区逻辑，按数值大小划分到不同 reducer
    public static class Partition extends Partitioner<IntWritable, IntWritable> {
        @Override
        public int getPartition(IntWritable key, IntWritable value, int numPartitions) {
            int maxValue = 65223; // 假设输入数据不超过该值
            int bound = maxValue / numPartitions + 1;
            int val = key.get();

            // 将整数值根据大小划分到对应的分区
            for (int i = 0; i < numPartitions; i++) {
                if (val >= bound * i && val < bound * (i + 1)) {
                    return i;
                }
            }
            return 0; // 默认放入第 0 分区（兜底逻辑）
        }
    }

    public static void main(String[] args) throws Exception {
        // 创建配置对象
        Configuration conf = new Configuration();

        // 设置 Hadoop 运行模式为 local，本地调试用（无需启动集群）
        conf.set("mapreduce.framework.name", "local");

        // 创建 Job 实例
        Job job = Job.getInstance(conf, "Merge and Sort");

        // 设置主类（用于打包 jar 时指明入口）
        job.setJarByClass(MyMapReduce.class);

        // 指定 Map、Reduce、Partition 类
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setPartitionerClass(Partition.class);

        // 指定输出键值类型（中间输出和最终输出一致）
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);

        // 输入输出路径（默认设置在项目根目录的 input/ 和 output/ 目录）
        String inputPath = "input";
        String outputPath = "output";

        // 如果 output 文件夹存在，先递归删除，防止 Hadoop 报错
        File outputDir = new File(outputPath);
        if (outputDir.exists()) {
            deleteDirectory(outputDir);
            System.out.println("output 目录存在，将其删除 ");
        }

        // 设置输入输出路径
        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        boolean success = job.waitForCompletion(true);  // 等待任务完成
        if (success) {
            System.out.println("运行结果已经写入 ./output 目录");
        } else {
            System.err.println("任务执行失败！");
        }
        System.exit(success ? 0 : 1);  // 最后再退出程序
    }

    // 删除目录及其所有内容的递归函数
    private static void deleteDirectory(File dir) {
        File[] files = dir.listFiles();
        if (files != null) {
            for (File file : files) {
                deleteDirectory(file);
            }
        }
        dir.delete();
    }
}