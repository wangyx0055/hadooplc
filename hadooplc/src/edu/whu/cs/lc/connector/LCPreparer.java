/**
 * @author chenbiren
 * @file LCPreparer.java
 * @date 2012-9-26
 **/
package edu.whu.cs.lc.connector;
import org.apache.hadoop.mapred.JobConf;
public interface LCPreparer {
	public String prepare(String lcquery,LCInputSplit split,JobConf conf);
}
