/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package final2;

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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 *
 * @author sumedh
 */
public class Final2 {

    /**
     * @param args the command line arguments
     */
    
    public static class BloomFilterMapper extends Mapper<Object,Text,Text,NullWritable>
    {
        Funnel<Flight> f = new Funnel<Flight>() {
            @Override
            public void funnel(Flight flight, Sink into) {
               into.putInt(flight.distance);
            }
        };
        
        private BloomFilter<Flight> flightFilter = BloomFilter.create(f, 500, 0.1);

       @Override
        protected void setup(Context context)  {
            
        
        
            ArrayList<Flight> flightList = new ArrayList<Flight>(); 
            flightList.add(new Flight(24));
            flightList.add(new Flight(28));
            flightList.add(new Flight(30));
            flightList.add(new Flight(31));
//            flightList.add(new Flight("2217"));
//            flightList.add(new Flight("2217"));
//            flightList.add(new Flight("2917"));
//            flightList.add(new Flight("2846"));
            
             for (Flight flight : flightList) {
                flightFilter.put(flight);
            }
   
            
         
                    
        }

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String values[] = value.toString().split(",");
           Text out = new Text(values[16].trim()+"    "+values[17].trim()+"   "+values[13].trim()+"    "+values[13].trim());
           
          
       
          if (NumberUtils.isNumber(values[16].trim()))
          {
              
             // if (Integer.parseInt(values[16].trim()) > 23 &&  Integer.parseInt(values[16].trim()) < 32)
//              {
//                  context.write(out, NullWritable.get());
//              }
              
          Flight f = new Flight(Integer.parseInt(values[16].trim()));
           if(flightFilter.mightContain(f))
            {
                context.write(out, NullWritable.get());
            }
          
         }
        
        
        }
     }
    public static void main(String[] args) {
                  Configuration conf = new Configuration();
     
        Job job;
        try {
            job = Job.getInstance(conf, "bloom filter");
        
        job.setJarByClass(Final2.class);
        job.setMapperClass(BloomFilterMapper.class);
         job.setMapOutputKeyClass(Text.class);
         job.setMapOutputValueClass(NullWritable.class);
         job.setNumReduceTasks(0);
    
    
      
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        boolean success = job.waitForCompletion(true);
        System.out.println(success);
       
        }
        catch (IOException|InterruptedException|ClassNotFoundException ex) {
            
        }
    }
    
}
