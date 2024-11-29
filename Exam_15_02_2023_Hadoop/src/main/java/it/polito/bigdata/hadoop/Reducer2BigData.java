package it.polito.bigdata.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Basic MapReduce Project - Reducer
 */
class Reducer2BigData extends Reducer<
        Text,
        IntWritable,
        Text,
        DoubleWritable> {

    @Override
    protected void reduce(
            Text key,
            Iterable<IntWritable> values,
            Context context) throws IOException, InterruptedException {
        
        //read tuples in format: (country, N)
        //compute the avg number of houses for each key (country)
        //discard countries where avg < 10000

        int sum = 0, num = 0;
        double avg = 0.0;
        int thres = 2;

        for (IntWritable v: values){
            num+=1;
            sum+=v.get();
        }

        avg = (double)sum / (double)num;

        if (avg >= thres) context.write(new Text(key), new DoubleWritable(avg));
    }
}
