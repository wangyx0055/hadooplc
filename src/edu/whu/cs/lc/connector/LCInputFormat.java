/**
 * @author chenbiren
 * @file LCInputFormat.java
 * @date 2012-10-8
 **/
package edu.whu.cs.lc.connector;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.lucene.queryParser.ParseException;

import edu.whu.cs.lc.benchmark.HostInfo;
import edu.whu.cs.lc.catalog.Catalog;

public abstract class LCInputFormat<T extends LCWritable> implements
					InputFormat<FloatWritable, T>, JobConfigurable {
	Log LOG = LogFactory.getLog(this.getClass());
	protected LCConfiguration lcConf;
	@Override
	public void configure(JobConf conf) {
		lcConf = new LCConfiguration();
	}
	
	@Override
	public RecordReader<FloatWritable, T> getRecordReader(InputSplit split,
			JobConf job, Reporter reporter) throws IOException {	
		String str = "getRecordReader: " + InetAddress.getLocalHost().getHostName();
		HostInfo.text = str;
		try {
			return new LCRecordReader<T>(lcConf, (LCInputSplit) split, job);
		} catch (ParseException e) {
			throw new RuntimeException(e);
		}
	
	}
	
	@Override
	public InputSplit[] getSplits(JobConf conf, int numSplits)
			throws IOException {

		Catalog.getInstance(conf).setSplitLocationStructure(lcConf, conf.get(LCConst.LC_RELATION_ID));
		Collection<LCChunk> chunks = lcConf.getChunks();
		InputSplit[] splits = new InputSplit[chunks.size()];
		LOG.info("getSplits: " + InetAddress.getLocalHost().getHostName());
		int i = 0;
		for (LCChunk chunk : chunks) {
			LCInputSplit split = new LCInputSplit();
			LOG.info("ID: " + chunk.getId());
			Collection<String> hosts = chunk.getLocations();
			Iterator<String> it = hosts.iterator();
			while(it.hasNext()) {
				LOG.info(it.next());
			}
			/*Collection<LCChunkHost> ohosts = chunk.getHosts();
			Iterator<LCChunkHost> oit = ohosts.iterator();
			while(oit.hasNext()) {
				System.out.println(oit.next().getHost());
			}*/
			split.setChunk(chunk);
			split.setRelation(lcConf.getRelation());

			splits[i] = split;
			i++;
		}

		return splits;
	}
}
