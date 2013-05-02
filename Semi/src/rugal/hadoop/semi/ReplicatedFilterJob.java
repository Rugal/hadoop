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
import org.codehaus.jackson.map.AnnotationIntrospector.Pair;

import rugal.hadoop.replicated.enhanced.GenericReplicatedJoin;


public class ReplicatedFilterJob extends GenericReplicatedJoin
{
    public static void main(String... args) throws Exception
    {
        runJob(new Path(args[0]), new Path(args[1]), new Path(args[2]));
    }

    public static void runJob(Path usersPath, Path uniqueUsersPath, Path outputPath)
            throws Exception
    {

        Configuration conf = new Configuration();
        conf.set("key.value.separator.in.input.line", Main.INPUT_SEPARATOR);

        FileSystem fs = uniqueUsersPath.getFileSystem(conf);

        FileStatus uniqueUserStatus = fs.getFileStatus(uniqueUsersPath);

        if (uniqueUserStatus.isDir())
        {
            for (FileStatus f : fs.listStatus(uniqueUsersPath))
            {
                if (f.getPath().getName().startsWith("part"))
                {
                    DistributedCache.addCacheFile(f.getPath().toUri(), conf);
                }
            }
        }
        else
        {
            DistributedCache.addCacheFile(uniqueUsersPath.toUri(), conf);
        }

        Job job = new Job(conf);

        job.setJarByClass(ReplicatedFilterJob.class);
        job.setMapperClass(ReplicatedFilterJob.class);

        job.setNumReduceTasks(0);

        job.setInputFormatClass(KeyValueTextInputFormat.class);

        outputPath.getFileSystem(conf).delete(outputPath, true);

        FileInputFormat.setInputPaths(job, usersPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        if (!job.waitForCompletion(true)) { throw new Exception("Job failed"); }
    }

    public Pair join(Pair inputSplitPair, Pair distCachePair)
    {
        return inputSplitPair;
    }
}
