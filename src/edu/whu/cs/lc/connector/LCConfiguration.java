/**
 * @author chenbiren
 * @file LCConfiguration.java
 * @date 2012-9-24
 **/
package edu.whu.cs.lc.connector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.JobConf;

import edu.whu.cs.lc.catalog.BaseLCConfiguration;

public class LCConfiguration extends BaseLCConfiguration{
	public static final Log LOG = LogFactory.getLog(LCConfiguration.class.getName());
	private JobConf jobConf;
	private String lcQuery;
	@SuppressWarnings("rawtypes")
	private Class valueClass;
	
	public LCConfiguration() {
		super();
	}
	
	public LCConfiguration(JobConf jobConf) {
		this.jobConf = jobConf;
	}
	public JobConf getJobConf() {
		return jobConf;
	}

	public void setJobConf(JobConf jobConf) {
		this.jobConf = jobConf;
	}
	
	public String getLcQuery() {
		return lcQuery;
	}
	
	public void setLcQuery(String lcQuery) {
		this.lcQuery = lcQuery;
	}
	
	@SuppressWarnings("rawtypes")
	public Class getValueClass() {
		return valueClass;
	}
	
	@SuppressWarnings("rawtypes")
	public void setValueClass(Class valueClass) {
		this.valueClass = valueClass;
	}
}
