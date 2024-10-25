package it.polito.bigdata.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Basic MapReduce Project - Reducer
 */
class CombinerBigData extends Reducer<
                Text,           // Input key type
                NullWritable,    // Input value type
                Text,           // Output key type
                NullWritable> {  // Output value type
    
    @Override
    
    protected void reduce(
        Text key, // Input key type
        Iterable<NullWritable> values, // Input value type
        Context context) throws IOException, InterruptedException {

        //just write the single word
        context.write(key, NullWritable.get());
    }
}
