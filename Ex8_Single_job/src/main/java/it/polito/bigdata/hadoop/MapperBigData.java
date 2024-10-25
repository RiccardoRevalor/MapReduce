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
                    MonthClass> {// Output value type
    
    protected void map(
            Text key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

            //at the end you'll emit key = year, value = monthincome class

            String[] date = key.toString().split("-");
            Text year = new Text(date[0]);
            String month = date[1];

            double income = Double.parseDouble(value.toString());

            context.write(year, new MonthClass(month, income));
    }
}
