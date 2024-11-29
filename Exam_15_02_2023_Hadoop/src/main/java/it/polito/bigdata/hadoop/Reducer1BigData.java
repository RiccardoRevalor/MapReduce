package it.polito.bigdata.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Basic MapReduce Project - Reducer
 */
class Reducer1BigData extends Reducer<
                Text,
                IntWritable,
                Text,
                IntWritable> {

    @Override
    protected void reduce(
        Text key,
        Iterable<IntWritable> values,
        Context context) throws IOException, InterruptedException {
        
        //for each city, compute the total houses per city
        //but in the key don't write the city, just write the country
        //because later on I have to computer the avg for country

        int sum = 0;

        for (IntWritable v: values){
            sum += v.get();
        }

        String country = key.toString().split("-")[1];
        context.write(new Text(country), new IntWritable(sum));
    }
}
