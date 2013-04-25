/**
 * @author chenbiren
 * @file LCInputSplit.java
 * @date 2012-9-26
 **/
package edu.whu.cs.lc.connector;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Collection;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InputSplit;

public class LCInputSplit implements InputSplit {
	public static final Log LOG = LogFactory.getLog(LCInputSplit.class.getName());
	
	protected String [] locations;
	protected LCChunk chunk;
	protected String relation;
	
	public LCChunk getChunk() {
		return chunk;
	}
	
	public void setChunk(LCChunk chunk) {
		this.chunk = chunk;
		setLocations();
	}
	
	public String getRelation() {
		return relation;
	}

	public void setRelation(String relation) {
		this.relation = relation;
	}
	
	private void setLocations() {
		Collection<LCChunkHost> hosts = chunk.getHosts();
		locations = new String[hosts.size()];
		int j = 0;
		for (LCChunkHost node : hosts) {
			locations[j] = node.getHost();
			System.out.println("location " + j + ": " + locations[j]);
			j++;
		}
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		relation = Text.readString(in);
		LOG.info("relation: " + relation);
		setChunk(deserializeChunk(in));
	}

	@Override
	public void write(DataOutput out) throws IOException {
		Text.writeString(out, relation);
		serializeChunk(chunk, out);
	}

	@Override
	public long getLength() throws IOException {
		return 1;
	}

	@Override
	public String[] getLocations() throws IOException {
		return locations;
	}
	
	private void serializeChunk(LCChunk chunk, DataOutput out)
			throws IOException {
		ByteArrayOutputStream byte_stream = new ByteArrayOutputStream();
		ObjectOutputStream object_stream = new ObjectOutputStream(
				byte_stream);
		object_stream.writeObject(chunk);
		object_stream.close();

		byte[] buf = byte_stream.toByteArray();
		BytesWritable bw = new BytesWritable(buf);
		bw.write(out);
	}
	
	private LCChunk deserializeChunk(DataInput in) throws IOException {
		BytesWritable br = new BytesWritable();
		br.readFields(in);
		byte[] buf = br.getBytes();
		ObjectInputStream byte_stream = new ObjectInputStream(
				new ByteArrayInputStream(buf));
		LCChunk chunk = null;
		try {
			chunk = (LCChunk) byte_stream.readObject();
		} catch (ClassNotFoundException e) {
			throw new IOException(e);
		}
		System.out.println("chunk:" + chunk.getId() + "," + chunk.getHosts().toArray().toString());
		return chunk;
	}
		
}
