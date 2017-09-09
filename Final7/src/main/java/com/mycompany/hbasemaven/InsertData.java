/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.hbasemaven;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 *
 * @author sumedh
 */
public class InsertData {
    
    public static void main(String[] args) throws Exception {
    
     Configuration config = HBaseConfiguration.create();
//
//      // Instantiating HTable class
     HTable hTable = new HTable(config, "Flight");
//

      Put p = new Put(Bytes.toBytes("11IADTPA")); 


      p.add(Bytes.toBytes("FlightDetails"),
      Bytes.toBytes("FlightNo"),Bytes.toBytes("335"));
//
      p.add(Bytes.toBytes("FlightDetails"),
      Bytes.toBytes("TailNo"),Bytes.toBytes("N432WN"));
      
      p.add(Bytes.toBytes("FlightDetails"),
      Bytes.toBytes("Carrier"),Bytes.toBytes("WN"));

      hTable.put(p);
      System.out.println("data inserted");

     
      
      // closing HTable
      hTable.close();

    
}
}
