package it.polito.bigdata.hadoop;

import java.io.IOException;

import org.apache.avro.JsonProperties.Null;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


/**
 * Basic MapReduce Project - Mapper
 */
class MapperBigData extends Mapper<
                    LongWritable, // Input key type
                    Text,         // Input value type
                    Text,         // Output key type
                    NullWritable> {// Output value type
    
    protected void map(
            LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

            //split the sentence to retrieve the different words
            String[] words = key.toString().split("\\s+");

            for (String word : words) {
                // emit the pair (word, 1)
                context.write(new Text(word), NullWritable.get());
            }
    }
}
