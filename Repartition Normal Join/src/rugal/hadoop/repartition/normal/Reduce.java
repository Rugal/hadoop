package rugal.hadoop.repartition.normal;

import org.apache.hadoop.io.Text;

import rugal.hadoop.repartition.normal.impl.DataJoinReducerBase;
import rugal.hadoop.repartition.normal.impl.IntermediateData;

public class Reduce extends DataJoinReducerBase
{

    @Override
    protected IntermediateData combine(Object[] tags, Object[] values)
    {
        if (tags.length < 2)
        {
            return null;
        }
        StringBuilder joinString = new StringBuilder();
        for (int i = 0; i < values.length; i++)
        {
            if (i > 0) joinString.append(Main.INPUT_SPLIT);
            TaggedWritable tw = (TaggedWritable) values[i];
            String line = ((Text) tw.getData()).toString();
            String[] tokens = line.split(Main.INPUT_SPLIT, 2);
            joinString.append(tokens[1]);
        }
        TaggedWritable value = new TaggedWritable(new Text(joinString.toString()));
        value.setTag((Text) tags[0]);
        return value;
    }
}
