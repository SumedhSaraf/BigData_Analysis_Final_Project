/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package final3;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 *
 * @author sumedh
 */
public class MonthPartitioner extends Partitioner<Text, CustomWritable> {

    @Override
    public int getPartition(Text key, CustomWritable value, int i) {
        int z = 0;
        Integer q = Integer.parseInt(key.toString());
        return q.intValue();
    }

  
    
    
}
