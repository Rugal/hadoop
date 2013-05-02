package rugal.hadoop.repartition.enhanced.impl;

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

/**
 * This abstract class serves as the base class for the mapper class of a data
 * join job.
 */
public abstract class OptimizedDataJoinMapperBase extends OptimizedJobBase
{

    protected String inputFile = null;

    protected String fileToBeBuffered = null;

    protected JobConf job = null;

    protected Text inputTag = null;

    protected Reporter reporter = null;

    protected CompositeKey outputKey = new CompositeKey();

    protected BooleanWritable smaller;

    @Override
    public void configure(JobConf job)
    {
        super.configure(job);
        this.job = job;
        this.inputFile = job.get("map.input.file");
        this.fileToBeBuffered = job.get("user.file.buffered");
        this.inputTag = generateInputTag(this.inputFile, fileToBeBuffered);
        if (isInputSmaller())
        {
            smaller = new BooleanWritable(true);
            outputKey.setOrder(0);
        }
        else
        {
            smaller = new BooleanWritable(false);
            outputKey.setOrder(1);
        }
    }

    /**
     * Determine the source tag based on the input file name.
     *
     * @param inputFile
     * @return the source tag computed from the given file name.
     */
    protected abstract Text generateInputTag(String inputFile, String fileToBeBuffered);

    /**
     * Generate an output value. The user code can also perform
     * projection/filtering. If it decides to discard the input record when
     * certain conditions are met,it can simply return a null.
     *
     * @param o
     *            the Map input value
     * @return an object of OutputValue computed from the given value.
     */
    protected abstract IntermediateData generateMapOutputValue(Object o);

    /**
     * Generate a map output key. The user code can compute the key
     * programmatically, not just selecting the values of some fields. In this
     * sense, it is more general than the joining capabilities of SQL.
     *
     * @param aRecord
     * @return the group key for the given record
     */
    protected abstract String generateGroupKey(Object key, IntermediateData aRecord);

    /**
     *
     * @param inputFile
     * @return true if the data from the supplied input file is smaller than
     *         data from the other input file.
     */
    protected abstract boolean isInputSmaller();

    @Override
    public void map(Object key, Object value, OutputCollector output, Reporter reporter)
            throws IOException
    {
        if (this.reporter == null)
        {
            this.reporter = reporter;
        }
        addLongValue("totalCount", 1);
        IntermediateData aRecord = generateMapOutputValue(value);
        if (aRecord == null)
        {
            addLongValue("discardedCount", 1);
            return;
        }
        aRecord.setSmaller(smaller);
        String groupKey = generateGroupKey(key, aRecord);
        if (groupKey == null)
        {
            addLongValue("nullGroupKeyCount", 1);
            return;
        }
        outputKey.setKey(groupKey);
        output.collect(outputKey, aRecord);
        addLongValue("collectedCount", 1);
    }

    @Override
    public void close() throws IOException
    {
        if (this.reporter != null)
        {
            this.reporter.setStatus(super.getReport());
        }
    }

    @Override
    public void reduce(Object arg0, Iterator arg1, OutputCollector arg2, Reporter arg3)
            throws IOException
    {
        // TODO Auto-generated method stub

    }
}
