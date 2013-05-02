package rugal.hadoop.replicated.enhanced;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Main
{
    public static final String INPUT_SEPARATOR=",";
    public static void main(String... args) throws Exception
    {
        runJob(args);
    }

    public static void runJob(String... args) throws Exception
    {
        Path smallFilePath = new Path(args[0]);
        Configuration conf = new Configuration();
        FileSystem fs = smallFilePath.getFileSystem(conf);
        FileStatus smallFilePathStatus = fs.getFileStatus(smallFilePath);
        if (smallFilePathStatus.isDir())
        {
            for (FileStatus f : fs.listStatus(smallFilePath))
            {
                if (f.getPath().getName().contains(args[0]))
                {
                    DistributedCache.addCacheFile(f.getPath().toUri(), conf);
                }
            }
        }
        else
        {
            DistributedCache.addCacheFile(smallFilePath.toUri(), conf);
        }
        Path inputPath = new Path(args[1]);
        Path outputPath = new Path(args[2]);
        Job job = new Job(conf);
        job.setJarByClass(Main.class);
        job.setMapperClass(GenericReplicatedJoin.class);
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setNumReduceTasks(0);
        outputPath.getFileSystem(conf).delete(outputPath, true);
        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);
        job.waitForCompletion(true);
    }

}
