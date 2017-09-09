/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package final5;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.TreeMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.commons.lang.math.NumberUtils;
/**
 *
 * @author sumedh
 */
public class Final5 {

  public static class TopMapper extends Mapper<Object,Text,
                                             NullWritable,Text>
  {
    
      private TreeMap<Integer,Text> reputation =  
            new TreeMap<Integer, Text>();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String row[] = value.toString().split(",");
            
           if (NumberUtils.isNumber(row[11].trim())) 
           {
            
            reputation.put(Integer.parseInt(row[11].toString())
                    , new Text(value.toString()));
           if (reputation.size() > 10)
           {
               reputation.remove(reputation.firstKey());
        }
        }
        }
        
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            
            for(Text t:reputation.descendingMap().values())
      {
          context.write(NullWritable.get(),t);
      }
      
        }
                
      
  }
  
  
  public static class Reduce1 extends Reducer<NullWritable,Text,NullWritable,Text>
    {

      private TreeMap<Integer, Text> repToRecordMap = new TreeMap<Integer, Text>();
      
      public void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
    
        {
            for (Text value : values) {
               
     repToRecordMap.put(Integer.parseInt(value.
             toString().split(",")[11]),new Text(value));
            }
     
            if (repToRecordMap.size() > 10)
            {
             repToRecordMap.remove(repToRecordMap.firstKey());
            }
            
            for(Text t:repToRecordMap.descendingMap().values())
            {
                context.write(NullWritable.get(), t);
            }
        }
        }

    



      
       
 
       
    }
    
    public static void main(String[] args) {
        
        
            
        Configuration conf = new Configuration();
     
        Job job;
        try {
         job = Job.getInstance(conf, "Topten");
         job.setJarByClass(Final5.class);
         job.setMapperClass(TopMapper.class);
         job.setMapOutputKeyClass(NullWritable.class);
         job.setMapOutputValueClass(Text.class);
       //  job.setNumReduceTasks(12);
         job.setReducerClass(Reduce1.class);
         job.setOutputKeyClass(NullWritable.class);
         job.setOutputValueClass(Text.class); 
         FileInputFormat.addInputPath(job, new Path(args[0]));
         FileOutputFormat.setOutputPath(job, new Path(args[1]));
         boolean success = job.waitForCompletion(true);
        
       
        }
        catch (IOException|InterruptedException|ClassNotFoundException ex) {
            
        }
        
    }
    
}
