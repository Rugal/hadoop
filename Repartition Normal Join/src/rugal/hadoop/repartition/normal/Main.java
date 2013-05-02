package rugal.hadoop.repartition.normal;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class Main
{
    public static final String OUTPUT_SPLIT = "|";
    public static final String INPUT_SPLIT = ",";
    public static int REDUCE_NUMER;

    public static void main(String[] args) throws Exception
    {
        JobConf job = new JobConf();
        job.setJarByClass(Main.class);
        Path in = new Path(args[0]);
        Path out = new Path(args[1]);
        REDUCE_NUMER = Integer.parseInt(args[2]);
        FileInputFormat.setInputPaths(job, in);
        FileOutputFormat.setOutputPath(job, out);
        job.setJobName("Normal Repartition Join");
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setInputFormat(TextInputFormat.class);
//        job.setInputFormat(KeyValueTextInputFormat.class);
        job.setOutputFormat(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(TaggedWritable.class);
        job.setNumReduceTasks(REDUCE_NUMER);
//        job.set("key.value.separator.in.input.line", INPUT_SPLIT);
        job.set("mapred.textoutputformat.separator", OUTPUT_SPLIT);
        JobClient.runJob(job);
    }
}