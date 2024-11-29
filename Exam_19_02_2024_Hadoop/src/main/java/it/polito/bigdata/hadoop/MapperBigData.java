package it.polito.bigdata.hadoop;

import java.io.IOException;

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



    private boolean checkDate(String d){
        int y0;
        y0 = Integer.parseInt(d.split("/")[0]);

        if (y0 < 2020 || y0 > 2023) return false;
        
        return true;

    }
    
    protected void map(
            LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

            // Split each sentence in words. Use whitespace(s) as delimiter 
    		// (=a space, a tab, a line break, or a form feed)
    		// The split method returns an array of strings
            String[] words = value.toString().split(",");
            
            String date = words[0].split("-")[0];

            //check wether date is right
            if (checkDate(date)){
                String uid = words[1];
                String pid = words[2];
                context.write(new Text(new String(uid + "," + pid)), NullWritable.get());
            }
            
    }
}
