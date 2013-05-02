package rugal.hadoop.repartition.normal;

import org.apache.hadoop.io.Text;

import rugal.hadoop.repartition.normal.impl.DataJoinMapperBase;
import rugal.hadoop.repartition.normal.impl.IntermediateData;

public class Map extends DataJoinMapperBase
{

    @Override
    protected Text generateInputTag(String string)
    {
        // to generate a tag for mark the input
        String ds = inputFile.substring(inputFile.lastIndexOf("/") + 1).split("-")[0];
        return new Text(ds);
    }

    @Override
    protected IntermediateData generateTaggedMapOutput(Object o)
    {
        // to wrap a tagged map value
        TaggedWritable value = new TaggedWritable((Text) o);
        value.setTag(inputTag);
        return value;
    }

    @Override
    protected Text generateGroupKey(IntermediateData tmo)
    {
        // to generate the KEY for joining
        String line = ((Text) tmo.getData()).toString();
        String[] tokens = line.split(Main.INPUT_SPLIT);
        String groupkey = tokens[0];
        return new Text(groupkey);
    }
}