/**
 * @author chenbiren
 * @file LCReader.java
 * @date 2012-9-25
 **/
package edu.whu.cs.lc.connector;

import java.io.IOException;
import java.sql.SQLException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.lucene.queryParser.ParseException;

public class LCRecordReader<T extends LCWritable> extends AbstractLCRecordReader implements RecordReader<FloatWritable, T> {
	public static final Log LOG = LogFactory.getLog(LCRecordReader.class.getName());
	
	private Class<T> valueClass;
	private JobConf conf;
	private LCConfiguration lcConf;
	
	public LCRecordReader(LCConfiguration lcConf,LCInputSplit split,JobConf conf) throws IOException, ParseException {
		this.lcConf = lcConf;
		this.valueClass = lcConf.getValueClass();
		this.conf = conf;
		
		setupLC(split,conf);
	}

	@Override
	public T createValue() {
		return ReflectionUtils.newInstance(valueClass, conf);
	}

	@Override
	public boolean next(FloatWritable key, T value) throws IOException {
		if (!results.next())
			return false;
			
		key.set(pos);
		LOG.info(results) ;
		value.getResult(results);
		pos++;
		
		return true;
	}

	@Override
	public String getLuceneQuery() {
		return lcConf.getLcQuery();
	}


	
}
