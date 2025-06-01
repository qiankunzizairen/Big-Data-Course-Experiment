//主要思想：
/**
 * 	Mapper：
 *  对于每一条记录，都拆成：
 *  <child, P:parent>
	<parent, C:child>
 * 
 */
/**
 * Reduce：
 * 对于每个key：
 * 聚合P、C两个属性的值
 */
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MyMapReduce {

    // Map 阶段：将每条 child-parent 转换成两条记录
    public static class Map extends Mapper<Object, Text, Text, Text> {
        private Text person = new Text();
        private Text relation = new Text();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // 读取一行数据，跳过表头和空行
            String line = value.toString().trim();
            if (line.startsWith("child") || line.isEmpty()) return;

            // 切分行内容，格式为 "child parent"
            String[] tokens = line.split("\\s+");
            if (tokens.length != 2) return;

            String child = tokens[0];
            String parent = tokens[1];

            // 发出 child → parent 的关系：key=child, value="PARENT:xxx"
            person.set(child);
            relation.set("PARENT:" + parent);
            context.write(person, relation);

            // 发出 parent ← child 的反向关系：key=parent, value="CHILD:xxx"
            person.set(parent);
            relation.set("CHILD:" + child);
            context.write(person, relation);
        }
    }

    // Reduce 阶段：将某人的 parent 和 child 组合，拼出孙子→爷爷
    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        private ArrayList<String> parents = new ArrayList<>();
        private ArrayList<String> children = new ArrayList<>();

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // 清空上一次 reduce 的临时列表
            parents.clear();
            children.clear();

            // 将输入区分成 parent 和 child 列表
            for (Text val : values) {
                String relation = val.toString();
                if (relation.startsWith("PARENT:")) {
                    parents.add(relation.substring(7)); // 去掉前缀 "PARENT:"
                } else if (relation.startsWith("CHILD:")) {
                    children.add(relation.substring(6)); // 去掉前缀 "CHILD:"
                }
            }

            // 将每个 child 和每个 parent 组合，输出孙子 → 爷爷
            for (String child : children) {
                for (String parent : parents) {
                    context.write(new Text(child), new Text(parent));
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        // 设置为 Hadoop 本地模式（适合学习）
        conf.set("mapreduce.framework.name", "local");

        Job job = Job.getInstance(conf, "Grandchild-Grandparent Finder");
        job.setJarByClass(MyMapReduce.class);

        // 设置 Map 和 Reduce 类
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        // 设置输出键值类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // 设置输入输出目录（可自定义）
        String inputPath = "input";
        String outputPath = "output";

        // 如果 output 目录存在，先删除它，防止报错
        File outputDir = new File(outputPath);
        if (outputDir.exists()) {
            deleteDirectory(outputDir);
            System.out.println("output 目录已存在，已删除旧目录");
        }

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        // 执行任务
        boolean success = job.waitForCompletion(true);
        if (success) {
            System.out.println("任务成功执行，结果写入 ./output");
        } else {
            System.err.println("任务执行失败！");
        }

        System.exit(success ? 0 : 1);
    }

    // 递归删除目录函数
    private static void deleteDirectory(File dir) {
        File[] files = dir.listFiles();
        if (files != null) {
            for (File f : files) {
                deleteDirectory(f);
            }
        }
        dir.delete();
    }
}