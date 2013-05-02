package rugal.hadoop.semi;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import rugal.hadoop.replicated.enhanced.GenericReplicatedJoin;

public class FinalJoinJob
{
    public static void main(String... args) throws Exception
    {
        runJob(new Path(args[0]), new Path(args[1]), new Path(args[2]));
    }

    /**
     * @param userLogsPath
     * @param usersPath
     * @param outputPath
     * @throws Exception
     */
    public static void runJob(Path userLogsPath, Path usersPath, Path outputPath) throws Exception
    {

        Configuration conf = new Configuration();
        conf.set("key.value.separator.in.input.line", Main.INPUT_SEPARATOR);

        FileSystem fs = usersPath.getFileSystem(conf);

        FileStatus usersStatus = fs.getFileStatus(usersPath);

        if (usersStatus.isDir())
        {
            for (FileStatus f : fs.listStatus(usersPath))
            {
                if (f.getPath().getName().startsWith("part"))
                {
                    DistributedCache.addCacheFile(f.getPath().toUri(), conf);
                }
            }
        }
        else
        {
            DistributedCache.addCacheFile(usersPath.toUri(), conf);
        }

        Job job = new Job(conf);

        job.setJarByClass(FinalJoinJob.class);
        job.setMapperClass(GenericReplicatedJoin.class);

        job.setNumReduceTasks(0);

        job.setInputFormatClass(KeyValueTextInputFormat.class);

        outputPath.getFileSystem(conf).delete(outputPath, true);

        FileInputFormat.setInputPaths(job, userLogsPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        if (!job.waitForCompletion(true)) { throw new Exception("Job failed"); }
    }
}
