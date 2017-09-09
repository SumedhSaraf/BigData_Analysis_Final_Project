/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package final3;

import com.google.common.base.Charsets;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnel;
import com.google.common.hash.Sink;
import java.io.IOException;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 *
 * @author sumedh
 */
public class Final3 {

   
    
    public static class Mapper1 extends Mapper<Object,Text,Text,CustomWritable>
    {
        //Text fightDetails = new Text();
        Text month = new Text();
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String values[] = value.toString().split(",");
            
            
             if (NumberUtils.isNumber(values[2].trim()) && NumberUtils.isNumber(values[9].trim()))
          {
             
            
             CustomWritable cw = new CustomWritable();
            month.set(values[1].toString());
            //Text cw = new Text(values[16].toString());
            cw.setOrigin(values[16].toString());
            cw.setDestination(values[17].toString());
            cw.setFlightNUmber(Integer.parseInt(values[9].toString()));
            cw.setFlight(values[8].toString());
            context.write(month, cw);
            
            
        }
        
        }
        
     }
    
      public static class Reduce1 extends Reducer<Text,CustomWritable,Text,CustomWritable>
    {

        @Override
        protected void reduce(Text key, Iterable<CustomWritable> values, Context context) throws IOException, InterruptedException {
            for (CustomWritable value : values) {
                //Text nw = new Text(value.toString());
                context.write(key,value);
            }
 
        }



      
       
 
       
    }
    
    
    public static void main(String[] args) {
        
        
        Configuration conf = new Configuration();
     
        Job job;
        try {
         job = Job.getInstance(conf, "Divide");
         job.setJarByClass(Final3.class);
         job.setMapperClass(Mapper1.class);
         job.setMapOutputKeyClass(Text.class);
         job.setMapOutputValueClass(CustomWritable.class);
         job.setNumReduceTasks(12);
         job.setReducerClass(Reduce1.class);
         job.setOutputKeyClass(Text.class);
         job.setOutputValueClass(CustomWritable.class); 
         FileInputFormat.addInputPath(job, new Path(args[0]));
         FileOutputFormat.setOutputPath(job, new Path(args[1]));
         boolean success = job.waitForCompletion(true);
        
       
        }
        catch (IOException|InterruptedException|ClassNotFoundException ex) {
            
        }
        
             
            
              
            
        }
        
        }
        
     
                  
    
    

