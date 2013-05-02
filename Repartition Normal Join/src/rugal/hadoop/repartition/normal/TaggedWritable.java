package rugal.hadoop.repartition.normal;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import rugal.hadoop.repartition.normal.impl.IntermediateData;

public class TaggedWritable extends IntermediateData
{

    public TaggedWritable()
    {
        tag = new Text();
    }

    private Writable data;

    public TaggedWritable(Writable data)
    {
        this.data = data;
        tag = new Text("");
    }

    public void setData(Writable data) {
        this.data = data;
    }

    @Override
    public Writable getData() {
        return data;
    }

    @Override
    public void write(DataOutput d) throws IOException {
        tag.write(d);
        data.write(d);
    }

    @Override
    public void readFields(DataInput di) throws IOException {
        tag.readFields(di);
        if (data == null) data = new Text();
        data.readFields(di);
    }
}
