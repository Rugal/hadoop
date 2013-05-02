package rugal.hadoop.replicated.normal;

import java.io.FileReader;
import java.io.IOException;
import java.util.Hashtable;
import java.util.Scanner;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class MapClass extends MapReduceBase implements Mapper<Text, Text, Text, Text>
{
    private Hashtable<String, String> joinData = new Hashtable<>();

    @Override
    public void configure(JobConf conf)
    {
        // System.out.println("Rugal Bernstein");
        try
        {
            Path[] cacheFiles = DistributedCache.getLocalCacheFiles(conf);
            if (cacheFiles != null && cacheFiles.length > 0)
            {

                String[] tokens;
                Scanner joinReader = new Scanner(new FileReader(cacheFiles[0].toString()));
                System.out.println(joinReader.hasNext());
                while (joinReader.hasNext())
                {
                    String line = joinReader.nextLine();
                    tokens = line.split(",", 2);

                    joinData.put(tokens[0], tokens[1]);
                }
                joinReader.close();
            }
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }

    @Override
    public void map(Text key, Text value, OutputCollector<Text, Text> output, Reporter reporter)
            throws IOException
    {
        if (joinData.containsKey(key))
        {
            String joinValue = joinData.get(key);
            output.collect(key, new Text(value.toString() + "," + joinValue));
        }
    }
}