package rugal.hadoop.repartition.enhanced;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;

import rugal.hadoop.repartition.enhanced.impl.CompositeKey;
import rugal.hadoop.repartition.enhanced.impl.CompositeKey.CompositeKeyComparator;
import rugal.hadoop.repartition.enhanced.impl.CompositeKeyOnlyComparator;
import rugal.hadoop.repartition.enhanced.impl.CompositeKeyPartitioner;

public class Main
{
    public static int REDUCE_NUMER;

    public static final String OUTPUT_SPLIT = "|";
    public static final String INPUT_SPLIT = ",";
    public static void main(String... args) throws Exception {
        JobConf job = new JobConf();
        job.setJarByClass(Main.class);
        String input = args[1];
        Path output = new Path(args[2]);
        REDUCE_NUMER = Integer.parseInt(args[3]);
        output.getFileSystem(job).delete(output, true);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setInputFormat(TextInputFormat.class);
        job.setMapOutputKeyClass(CompositeKey.class);
        job.setMapOutputValueClass(TaggedWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setPartitionerClass(CompositeKeyPartitioner.class);
        job.setOutputKeyComparatorClass(CompositeKeyComparator.class);
        job.setOutputValueGroupingComparator(CompositeKeyOnlyComparator.class);
        job.set("user.file.buffered", args[0]);
        job.setNumReduceTasks(REDUCE_NUMER);
        FileInputFormat.setInputPaths(job, input);
        FileOutputFormat.setOutputPath(job, output);
        JobClient.runJob(job);
    }
}
