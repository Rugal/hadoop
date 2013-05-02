package rugal.hadoop.repartition.enhanced;

import org.apache.hadoop.io.Text;

import rugal.hadoop.repartition.enhanced.impl.IntermediateData;
import rugal.hadoop.repartition.enhanced.impl.OptimizedDataJoinMapperBase;

public class Map extends OptimizedDataJoinMapperBase
{
    private boolean smaller;

    @Override
    protected Text generateInputTag(String inputFile, String fileToBeBuffered)
    {
        // tag the row with input file name (data source)
        String ds = inputFile.substring(inputFile.lastIndexOf("/") + 1).split("-")[0];
        // System.out.println(ds);
        smaller = inputFile.contains(fileToBeBuffered);
        return new Text(ds);
    }

    @Override
    protected String generateGroupKey(Object key, IntermediateData output)
    {
        String line = (output.getData()).toString();
        String[] tokens = line.split(Main.INPUT_SPLIT);
        return new String(tokens[0]);
    }

    @Override
    protected boolean isInputSmaller()
    {
        return smaller;
    }

    @Override
    protected IntermediateData generateMapOutputValue(Object o)
    {
        return new TaggedWritable((Text) o);
    }
}