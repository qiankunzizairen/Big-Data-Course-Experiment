//主要思想：
/**
 * 	Mapper：
 *  输入： key 是行的偏移量，value 是文本行，<, 1>
	处理： value作为 key 输出，value置空
	输出： 输出格式是 <文本行, >。
 */
/**
 * Reduce：
 * 输入： <文本行, >
   处理： reduce key
   输出： 输出格式是 <文本行, >。
 */

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MyMapReduce {

    /**
     * Mapper 类：继承 Hadoop 提供的 Mapper 类
     * 输入类型为 <Object, Text>：Object 是偏移量，Text 是文件的一行
     * 输出类型为 <Text, Text>：把每一行当作 key，value 设为 ""（目的是去重）
     */
    public static class Map extends Mapper<Object, Text, Text, Text> {
        private static Text text = new Text();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // 将读取的一行数据作为 key 输出，value 设置为空字符串
            text.set(value.toString());
            context.write(text, new Text(""));
        }
    }

    /**
     * Reducer 类：继承 Hadoop 提供的 Reducer 类
     * 对 key 分组后只输出一次（key 就是原始文本行），达到去重目的
     */
    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // 不管 value 有几个（表示出现了多少次），都只输出一次 key
            context.write(key, new Text(""));
        }
    }

    public static void main(String[] args) throws Exception {
        // 1. 创建配置对象
        Configuration conf = new Configuration();

        // 设置为本地运行模式（不依赖 YARN / 集群）
        conf.set("mapreduce.framework.name", "local");

        // 2. 创建 MapReduce Job 实例，并命名
        Job job = Job.getInstance(conf, "Merge and duplicate removal");

        // 设置主类
        job.setJarByClass(MyMapReduce.class);

        // 3. 设置 Map 和 Reduce 的处理类
        job.setMapperClass(Map.class);
        job.setCombinerClass(Reduce.class);  // 可选：本例中 Reducer 和 Combiner 一样
        job.setReducerClass(Reduce.class);

        // 4. 设置输出键值类型（中间结果和最终输出都一样）
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // 5. 指定输入输出路径（本地路径）
        String inputPath = "input";
        String outputPath = "output";

        // 在运行前，检查 output 目录是否存在，若存在则删除（防止 Hadoop 报错）
        File outputDir = new File(outputPath);
        if (outputDir.exists()) {
            deleteDirectory(outputDir);
            System.out.println("output 目录存在，将其删除 ");
        }

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

    /**
     * 递归删除目录及其内容
     */
    private static void deleteDirectory(File dir) {
        File[] contents = dir.listFiles();
        if (contents != null) {
            for (File file : contents) {
                deleteDirectory(file);
            }
        }
        dir.delete();
    }
}