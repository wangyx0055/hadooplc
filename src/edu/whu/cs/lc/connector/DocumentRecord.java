package edu.whu.cs.lc.connector;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;

public class DocumentRecord implements Writable ,LCWritable{
	public static final Log LOG = LogFactory.getLog(DocumentRecord.class.getName());
	private FloatWritable key = new FloatWritable();// key
	private MapWritable value = new MapWritable() ;// value

	public FloatWritable getKey() {
		return key;
	}

	public MapWritable getValue() {
		return value;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		this.key.write(out) ;
		this.value.write(out) ;
	}

	public void getResult(ResultSet result) {
		// get key and value from hits
		LOG.info(result.getKey()) ;
		this.key.set(result.getKey()) ;
		LOG.info(result.getValue()) ;
		this.value.putAll(result.getValue()) ;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.key.readFields(in) ;
		this.value.readFields(in) ;
	}

}
