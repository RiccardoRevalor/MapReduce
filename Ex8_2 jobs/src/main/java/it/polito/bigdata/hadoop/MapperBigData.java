package it.polito.bigdata.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


/**
 * Basic MapReduce Project - Mapper
 */
class MapperBigData extends Mapper<
                    Text, // Input key type
                    Text,         // Input value type
                    Text,         // Output key type
                    IntWritable> {// Output value type
    
    protected void map(
            Text key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

            //ex line: 2015-11-01	1000
            //take just year and month for the key
            String[] date = key.toString().split("-");
            Text newKey = new Text(new String(date[0] + "-" + date[1]));
            //write the key and the value
            context.write(newKey, new IntWritable(Integer.parseInt(value.toString())));
    }
}
