package it.polito.bigdata.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
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
    Path outputDir1, outputDir2;
    int numberOfReducers1, numberOfReducers2;
	int exitCode;  
	
	// Parse the parameters
	// Number of instances of the reducer class 
    numberOfReducers1 = Integer.parseInt(args[0]);
    // Folder containing the input data
    inputPath = new Path(args[1]);
    // Output folder
    outputDir1 = new Path(args[2]);
    
    Configuration conf = this.getConf();

    // Define a new job
    Job job = Job.getInstance(conf); 


    // Assign a name to the job
    job.setJobName("Job1");
    
    // Set path of the input file/folder (if it is a folder, the job reads all the files in the specified folder) for this job
    FileInputFormat.addInputPath(job, inputPath);
    
    // Set path of the output folder for this job
    FileOutputFormat.setOutputPath(job, outputDir1);
    
    // Specify the class of the Driver for this job
    job.setJarByClass(DriverBigData.class);
    
    
    // Set job input format
    job.setInputFormatClass(TextInputFormat.class);

    // Set job output format
    job.setOutputFormatClass(TextOutputFormat.class);
       
    // Set map class
    job.setMapperClass(MapperBigData.class);
    
    // Set map output key and value classes
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(NullWritable.class);
    
    // Set reduce class
    job.setReducerClass(ReducerBigData.class);
        
    // Set reduce output key and value classes
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    // Set number of reducers
    job.setNumReduceTasks(numberOfReducers1);
    
    
    // Execute the job and wait for completion
    if (job.waitForCompletion(true)==true) {


      System.out.println("JOB1 FINISHED SUCCESFULLY.");
      //execute second job

      numberOfReducers2 = Integer.parseInt(args[3]);
      // Folder containing the input data
      inputPath = outputDir1;
      // Output folder
      outputDir2 = new Path(args[4]);

      // Define a new job
      Job job1 = Job.getInstance(conf); 

      // Assign a name to the job
      job1.setJobName("Job2");
      
      // Set path of the input file/folder (if it is a folder, the job reads all the files in the specified folder) for this job
      FileInputFormat.addInputPath(job1, inputPath);
      
      // Set path of the output folder for this job
      FileOutputFormat.setOutputPath(job1, outputDir2);
      
      // Specify the class of the Driver for this job
      job1.setJarByClass(DriverBigData.class);
      
      
      // Set job input format
      job1.setInputFormatClass(TextInputFormat.class);

      // Set job output format
      job1.setOutputFormatClass(TextOutputFormat.class);
        
      // Set map class
      job1.setMapperClass(MapperBigData2.class);
      
      // Set map output key and value classes
      job1.setMapOutputKeyClass(Text.class);
      job1.setMapOutputValueClass(IntWritable.class);
      
      // Set reduce class
      job1.setReducerClass(ReducerBigData2.class);
          
      // Set reduce output key and value classes
      job1.setOutputKeyClass(Text.class);
      job1.setOutputValueClass(NullWritable.class);

      // Set number of reducers
      job1.setNumReduceTasks(numberOfReducers2);
      
      
      // Execute the job and wait for completion
      if (job1.waitForCompletion(true)==true) {

        System.out.println("JOB2 FINISHED SUCCESFULLY");
        exitCode=0;
      } else {
        exitCode=1;
        System.out.println("ERROR IN JOB2");
        
      return exitCode;
      }
    }else
    	exitCode=1;
      System.out.println("ERROR IN JOB1");

    	
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