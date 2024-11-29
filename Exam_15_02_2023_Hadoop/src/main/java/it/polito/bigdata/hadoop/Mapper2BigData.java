package it.polito.bigdata.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


/**
 * Basic MapReduce Project - Mapper
 */
class Mapper2BigData extends Mapper<
        Text,
        Text,
        Text,
        IntWritable> {

    protected void map(
            Text key,
            Text value,
            Context context) throws IOException, InterruptedException {

        //just pass to the reducer 2 the tuples (Country, housesinCity)

        int houses = Integer.parseInt(value.toString());

        context.write(new Text(key), new IntWritable(houses));
    }
}
