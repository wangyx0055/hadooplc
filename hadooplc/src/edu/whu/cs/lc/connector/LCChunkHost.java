/**
 * @author chenbiren
 * @file LuceneChunkInfo.java
 * @date 2012-9-24
 **/
package edu.whu.cs.lc.connector;

import java.io.Serializable;

public class LCChunkHost implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 2849868834986905754L;
	
	private String path;
	private String analyzer;
	private String address;
	private String version;
	private String url;
	
	public LCChunkHost(String address,String url,String path,
			String analyzer,String version) {
		this.path = path;
		this.address = address;
		this.url = url;
		this.analyzer = analyzer;
		this.version = version;
	}
	
	public String getHost() {
		return address;
	}
	
	public void setHost(String host) {
		this.address = host;
	}
	
	public String getPath() {
		return path;
	}
	
	public void setPath(String path) {
		this.path = path;
	}
	
	public String getUrl() {
		return url;
	}
	
	public void setUrl(String url) {
		this.url = url;
	}
	
	public String getAnalyzer() {
		return analyzer;
	}
	
	public void setAnalyzer(String analyzer) {
		this.analyzer = analyzer;
	}
	
	public String getVersion() {
		return version;
	}
	
	public void setVersion(String version) {
		this.version = version;
	}
}

