package rugal.hadoop.replicated.normal;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class Main extends Configured
{
    public static final String OUTPUT_SPLIT = "|";
    public static final String INPUT_SPLIT = ",";

    /**
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception
    {
        JobConf job = new JobConf();
        DistributedCache.addCacheFile(new Path(args[0]).toUri(), job);
        Path in = new Path(args[1]);
        Path out = new Path(args[2]);
        FileInputFormat.setInputPaths(job, in);
        FileOutputFormat.setOutputPath(job, out);
        job.setJobName("Normal replicated join");
        job.setMapperClass(MapClass.class);
        job.setNumReduceTasks(0);
        job.setInputFormat(KeyValueTextInputFormat.class);
        job.setOutputFormat(TextOutputFormat.class);
        job.set("key.value.separator.in.input.line", INPUT_SPLIT);
        JobClient.runJob(job);
    }
}
