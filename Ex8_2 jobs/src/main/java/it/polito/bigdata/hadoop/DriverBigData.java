package it.polito.bigdata.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


/**
 * MapReduce program
 */
public class DriverBigData extends Configured 
implements Tool {

  @Override
  public int run(String[] args) throws Exception {

    Path inputPath;
    Path outputDir, outputDir2;
    int numberOfReducers1, numberOfReducers2;
	  int exitCode;  
	
	// Parse the parameters
	// Number of instances of the reducer class 
    numberOfReducers1 = Integer.parseInt(args[0]);
    // Folder containing the input data
    inputPath = new Path(args[1]);
    // Output folder
    outputDir = new Path(args[2]);
    
    Configuration conf = this.getConf();

    // Define a new job
    Job job = Job.getInstance(conf); 

    // Assign a name to the job
    job.setJobName("EX8_First_Job");
    
    // Set path of the input file/folder (if it is a folder, the job reads all the files in the specified folder) for this job
    FileInputFormat.addInputPath(job, inputPath);
    
    // Set path of the output folder for this job
    FileOutputFormat.setOutputPath(job, outputDir);
    
    // Specify the class of the Driver for this job
    job.setJarByClass(DriverBigData.class);
    
    
    // Set job input format
    job.setInputFormatClass(KeyValueTextInputFormat.class);

    // Set job output format
    job.setOutputFormatClass(TextOutputFormat.class);
       
    // Set map class
    job.setMapperClass(MapperBigData.class);
    
    // Set map output key and value classes
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);
    
    // Set reduce class
    job.setReducerClass(ReducerBigData.class);
        
    // Set reduce output key and value classes
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    // Set number of reducers
    job.setNumReduceTasks(numberOfReducers1);
    
    
    // Execute the job and wait for completion
    if (job.waitForCompletion(true)==true) {

      //set up the seconds job for calculating the average income for each year

      numberOfReducers2 = Integer.parseInt(args[3]);

      // Output folder for second job
      outputDir2 = new Path(args[4]);
      
      conf = this.getConf();

      // Define a new job2
      Job job2 = Job.getInstance(conf); 

      // Assign a name to the job2
      job2.setJobName("EX8_Second_Job");
      
      // Set path of the input file/folder (if it is a folder, the job2 reads all the files in the specified folder) for this job2
      FileInputFormat.addInputPath(job2, outputDir);
      
      // Set path of the output folder for this job2
      FileOutputFormat.setOutputPath(job2, outputDir2);
      
      // Specify the class of the Driver for this job2
      job2.setJarByClass(DriverBigData.class);
      
      
      // Set job2 input format
      job2.setInputFormatClass(KeyValueTextInputFormat.class);

      // Set job2 output format
      job2.setOutputFormatClass(TextOutputFormat.class);
        
      // Set map class
      job2.setMapperClass(MapperBigData2.class);
      
      // Set map output key and value classes
      job2.setMapOutputKeyClass(Text.class);
      job2.setMapOutputValueClass(IntWritable.class);
      
      // Set reduce class
      job2.setReducerClass(ReducerBigData2.class);
          
      // Set reduce output key and value classes
      job2.setOutputKeyClass(Text.class);
      job2.setOutputValueClass(IntWritable.class);

      // Set number of reducers
      job2.setNumReduceTasks(numberOfReducers2);

      if (job2.waitForCompletion(true)==true) {
        System.out.println("Job2 completed successfully");
        exitCode = 0;
      } else {
        System.out.println("Job2 failed");
        exitCode = 1;
      }
    }
    else
      System.out.println("Job1 failed");
    	exitCode=1;
    	
    return exitCode;
  }
  

  /** Main of the driver
   */
  
  public static void main(String args[]) throws Exception {
	// Exploit the ToolRunner class to "configure" and run the Hadoop application
    int res = ToolRunner.run(new Configuration(), 
    		new DriverBigData(), args);

    System.exit(res);
  }
}