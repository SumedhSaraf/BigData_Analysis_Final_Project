/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package final3;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

/**
 *
 * @author sumedh
 */
public class CustomWritable implements Writable,WritableComparable<CustomWritable> {
   
       
        private Integer flightNUmber;
        private String origin;
        private String destination;
        private String flight;

    public String getFlight() {
        return flight;
    }

    public void setFlight(String flight) {
        this.flight = flight;
    }
        
    public CustomWritable(Integer flightNUmber, String origin, String destination,String flight) {
        this.flightNUmber = flightNUmber;
        this.origin = origin;
        this.destination = destination;
        this.flight = flight;
    }

       public CustomWritable() {
    
    }

        
 
        
    public Integer getFlightNUmber() {
        return flightNUmber;
    }

    public void setFlightNUmber(Integer flightNUmber) {
        this.flightNUmber = flightNUmber;
    }

    

    public String getOrigin() {
        return origin;
    }

    public void setOrigin(String origin) {
        this.origin = origin;
    }

    public String getDestination() {
        return destination;
    }

    public void setDestination(String destination) {
        this.destination = destination;
    }

       
        
                

		@Override
		public void readFields(DataInput in) throws IOException {
                    
                        flightNUmber = WritableUtils.readVInt(in);
                        origin =  WritableUtils.readString(in);
                        destination = WritableUtils.readString(in);
                        flight = WritableUtils.readString(in);
                         
                   
    }

    
                                

		

		@Override
		public void write(DataOutput out) throws IOException
                    {
                         WritableUtils.writeVInt(out, flightNUmber);
                         WritableUtils.writeString(out, origin);
                         WritableUtils.writeString(out, destination);
                         WritableUtils.writeString(out, flight);
			
                    }

        @Override
        public String toString() {
            
            return (new StringBuilder().append(String.valueOf(flightNUmber)).append("\t").
                    append(origin).append("\t").append(destination).append("\t").append(flight).toString());
                   
            
        }

    @Override
    public int compareTo(CustomWritable o) {
        
         int result = flightNUmber.compareTo(o.flightNUmber);
         return result;
       
    }
                
             
	}
    
    
    
    
    
    

