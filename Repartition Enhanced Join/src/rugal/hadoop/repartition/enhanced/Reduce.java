package rugal.hadoop.repartition.enhanced;

import org.apache.hadoop.io.Text;

import rugal.hadoop.repartition.enhanced.impl.IntermediateData;
import rugal.hadoop.repartition.enhanced.impl.OptimizedDataJoinReducerBase;

public class Reduce extends OptimizedDataJoinReducerBase
{

    private TaggedWritable output = new TaggedWritable();
    private Text textOutput = new Text();

    @Override
    protected IntermediateData combine(String key, IntermediateData smallValue,
            IntermediateData largeValue)
    {
        if (smallValue == null || largeValue == null) { return null; }
        StringBuilder sb = new StringBuilder();
        sb.append(key);
        sb.append(Main.OUTPUT_SPLIT);
        sb.append(smallValue.getData().toString().split(Main.INPUT_SPLIT, 2)[1]);
        sb.append(Main.INPUT_SPLIT);
        sb.append(largeValue.getData().toString().split(Main.INPUT_SPLIT, 2)[1]);
        textOutput.set(sb.toString());
        output.setData(textOutput);
        return output;
    }
}