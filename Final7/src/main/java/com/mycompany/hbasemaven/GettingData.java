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

public class GettingData {

    public static void main(String args[]) throws IOException
    {
      Configuration config = HBaseConfiguration.create();

      // Instantiating HTable class
      HTable table = new HTable(config, "Flight");

      // Instantiating Get class
      Get g = new Get(Bytes.toBytes("11IADTPA"));

      // Reading the data
      Result result = table.get(g);

      // Reading values from Result class object
      byte [] value = result.getValue(Bytes.toBytes("FlightDetails"),Bytes.toBytes("FlightNo"));

      byte [] value1 = result.getValue(Bytes.toBytes("FlightDetails"),Bytes.toBytes("TailNo"));
      
      byte [] value2 = result.getValue(Bytes.toBytes("FlightDetails"),Bytes.toBytes("Carrier"));
      

      // Printing the values
      String FlightNo = Bytes.toString(value);
      String TailNo = Bytes.toString(value1);
      String Carrier = Bytes.toString(value2);
      
      System.out.println("flightno: " + FlightNo + " TailNo: " + TailNo + "Carrier:" + Carrier);
              
              
              
              
              
              
              
              
              
              
              
              
              
              
              
              
              


    }
	}


