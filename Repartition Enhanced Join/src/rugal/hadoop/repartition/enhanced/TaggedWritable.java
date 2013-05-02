package rugal.hadoop.repartition.enhanced;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import rugal.hadoop.repartition.enhanced.impl.IntermediateData;

public class TaggedWritable extends IntermediateData
{

    private Writable data;

    public TaggedWritable()
    {
        this.data = new Text("");
    }

    public TaggedWritable(Text data)
    {
        this.data = data;
    }

    public Writable getData() {
        return data;
    }

    public void setData(Text data) {
        this.data = data;
    }

    public void write(DataOutput out) throws IOException {
        this.smaller.write(out);
        this.data.write(out);
    }

    public void readFields(DataInput in) throws IOException {
        this.smaller.readFields(in);
        if (data == null) data = new Text();
        this.data.readFields(in);
    }
}
