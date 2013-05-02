package rugal.hadoop.replicated.enhanced;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.apache.commons.lang.StringUtils;

public class TextDistributedCacheFileReader implements DistributedCacheFileReader<String, String>,
        Iterator<Pair<String, String>>
{
    LineIterator iter;

    @Override
    public void init(File f) throws IOException
    {
        iter = FileUtils.lineIterator(f);
    }

    @Override
    public void close()
    {
        iter.close();
    }

    @Override
    public Iterator<Pair<String, String>> iterator()
    {
        return this;
    }

    @Override
    public boolean hasNext()
    {
        return iter.hasNext();
    }

    @Override
    public Pair<String, String> next()
    {
        String line = iter.next();
        Pair<String, String> pair = new Pair<String, String>();
        String[] parts = StringUtils.split(line, Main.INPUT_SEPARATOR, 2);
        pair.setKey(parts[0]);
        if (parts.length > 1)
        {
            pair.setData(parts[1]);
        }
        return pair;
    }

    @Override
    public void remove()
    {
        throw new UnsupportedOperationException();
    }
}
