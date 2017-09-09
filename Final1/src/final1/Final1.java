/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package final1;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
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

/**
 *
 * @author sumedh
 */
public class Final1 {
    
             	public static class AverageCountTuple implements Writable {

		private int average;
		private int count;

        public int getAverage() {
            return average;
        }

        public void setAverage(int average) {
            this.average = average;
        }

        public int getCount() {
            return count;
        }

        public void setCount(int count) {
            this.count = count;
        }

                

		@Override
		public void readFields(DataInput in) throws IOException {
			average = in.readInt();
			count = in.readInt();

		}

		@Override
		public void write(DataOutput out) throws IOException {
			// TODO Auto-generated method stub
			out.writeInt(average);
                        out.writeInt(count);
		}

        @Override
        public String toString() {
            return String.valueOf(this.average);
        }
                
             
	}
    
        public static class Map1 extends Mapper<Object, Text, Text, AverageCountTuple>{
        IntWritable delaytimeh = new IntWritable();
        Text carrierh = new Text();
        
            
        public void map(Object key, Text value, Context context){
            String carrier;
            Integer delaytime;
            try{
                AverageCountTuple out = new AverageCountTuple();
                String row[] = value.toString().split(",");
                 carrier = row[8];
                 delaytime = Integer.parseInt(row[15]);
                 carrierh.set(carrier);
                 delaytimeh.set(delaytime);
                 out.setCount(1);
                 out.setAverage(delaytime);
                 context.write(carrierh, out);
               
            }catch(Exception e){
                
                
            }
        }       
    }

         public static class Reduce1 extends Reducer<Text,AverageCountTuple,Text,AverageCountTuple>{
        
        private IntWritable totalDelay = new IntWritable();
        private IntWritable res = new IntWritable();
        private AverageCountTuple newrs = new AverageCountTuple();

           
            protected void reduce(Text key, Iterable<AverageCountTuple> values, Context context) throws IOException, InterruptedException {
               //int sum = 0;
               int count = 0;
               int avg = 0;
               
               
                        
			int sum = 0;
                        int cnt = 0;
			for (AverageCountTuple val : values) {
				sum += val.getCount()* val.getAverage();
				cnt += val.getCount();
			}
                         if (cnt != 0)
                         {
			avg = sum / cnt;
                         }
			res.set(avg);
                        newrs.setAverage(avg);
                        
			//res.setCount(cnt);
               
               
               
               
               
//                for (IntWritable val : values) {
//                    
//                  sum += val.get();
//                  count = count + 1;                    
//                }
               /// totalDelay.set(sum/cnt);
                context.write(key, newrs);
            }
        
        
    }
         

         
         
         
         
   
         
         public static class JoinMapper1 extends Mapper<Object,Text,Text,Text>
  {
      private Text outKey = new Text();
      private Text outValue = new Text();

        //@Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            
            
            String[] separatedInput = value.toString().split("\t");
         
                
            String id = separatedInput[0].trim();
        System.out.println(id);
            if (id == null)
            {
                return;
            }
            outKey.set(id);
            outValue.set("@"+value );
            context.write(outKey, outValue);
        
        
  }
  }
  
  public static class JoinMapper2 extends Mapper<Object, Text, Text, Text>
  {
       private Text outKey = new Text();
      private Text outValue = new Text();

        //@Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            
               String id = value.toString().split(",")[0].trim();
               String Id = id.substring(1,id.length()-1);
               System.out.println(Id);
           // String id = separatedInput[0];
            if (id == null)
            {
                return;
            }
            outKey.set(Id);
            outValue.set("$"+value );
            context.write(outKey, outValue);
            
        }
      
  }
  
  public static class JoinReducer extends Reducer<Text,Text,Text,Text>
          {
              private static final Text EMPTY_TEXT = new Text();
              private Text tmp = new Text();
              
              private ArrayList<Text> listA = new ArrayList<Text>();
                            private ArrayList<Text> listB = new ArrayList<Text>();
                            private String joinType = null;


       // @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
           listA.clear();
           listB.clear();
           
           while(values.iterator().hasNext())
           {
               tmp = values.iterator().next();
               if (tmp.charAt(0) == '@')
               {
                   listA.add(new Text(tmp.toString().substring(1)));
               } else if (tmp.charAt(0) == '$')
               {
                   listB.add(new Text(tmp.toString().substring(1)));
               }
           }
           
           executeJoinLogic(context);
           
           }

        private void executeJoinLogic(Context context) throws IOException, InterruptedException {
      
           
                if(!listA.isEmpty() && !listB.isEmpty())
                {
                for (Text A : listA) {
                   
                    {
                        for (Text B : listB) {
                            context.write(A,B);
                        
                    }
                   
                }
            }
            }
                
       
        
        
        
                            
                            
                            
          }
  }
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        
    

        
            Configuration conf = new Configuration();
            Job job1 = Job.getInstance(conf,"Highest Delay");
            job1.setJarByClass(Final1.class);
            job1.setMapperClass(Map1.class);
            job1.setMapOutputKeyClass(Text.class);
            job1.setMapOutputValueClass(AverageCountTuple.class);
            job1.setCombinerClass(Reduce1.class);
            job1.setReducerClass(Reduce1.class);
            job1.setOutputKeyClass(Text.class);
            job1.setOutputValueClass(AverageCountTuple.class);
            
            FileInputFormat.addInputPath(job1,new Path(args[0]));
            FileOutputFormat.setOutputPath(job1, new Path(args[1]));
            //System.exit(job1.waitForCompletion(true) ? 0:1);
            
            
            
            //FileOutputFormat.setOutputPath(job1, new Path(args[1]));
      
          boolean complete = job1.waitForCompletion(true);
     
     
     Configuration conf2 = new Configuration();
     Job job2 = Job.getInstance(conf2 ,"chaining");
     if(complete)
     {
         
         
    /// Job job = Job.getInstance(conf, "ReduceJoin");
      job2.setJarByClass(Final1.class);
      MultipleInputs.addInputPath(job2, new Path(args[1]), TextInputFormat.class, JoinMapper1.class);
      MultipleInputs.addInputPath(job2, new Path(args[2]), TextInputFormat.class, JoinMapper2.class); 
      job2.setMapOutputKeyClass(Text.class);
      job2.setMapOutputValueClass(Text.class);
    //  job2.getConfiguration().set("join.type","inner");
      job2.setReducerClass(JoinReducer.class);
      job2.setOutputFormatClass(TextOutputFormat.class);
      
      TextOutputFormat.setOutputPath(job2,new Path(args[3]));
      job2.setOutputKeyClass(Text.class);
      job2.setOutputValueClass(Text.class);
      
       System.exit(job2.waitForCompletion(true) ? 0:1);
 //     boolean success = job.waitForCompletion(true);
   //   return success ? 0:2;    
         
         
         

     }
         
        
        
        
       
    }
    
  }
  
