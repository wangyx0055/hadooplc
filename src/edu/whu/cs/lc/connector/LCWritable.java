/**
 * @author chenbiren
 * @file LCWritable.java
 * @date 2012-9-27
 **/
package edu.whu.cs.lc.connector;

import org.apache.lucene.search.TopDocs;


public interface LCWritable {
	
	public void getResult(ResultSet result);
	
}
