/**
 * @author chenbiren
 * @file LuceneChunk.java
 * @date 2012-9-24
 **/
package edu.whu.cs.lc.connector;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class LCChunk implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = -600444355826586946L;
	
	private static final Log LOG = LogFactory.getLog(LCChunk.class.getName());
	
	private static Random R = new Random(System.currentTimeMillis());

	private String id;
	
	private HashMap<String,LCChunkHost> locations = new HashMap<String,LCChunkHost>();
	
	public LCChunk(String id) {
		this.id = id;
	}
	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}
	
	public void addHost(LCChunkHost host) {
		locations.put(host.getHost(), host);
	}
	
	public LCChunkHost getHost(String host) {
		return locations.get(host);
	}
	
	public LCChunkHost getAnyHost() {
		int size = locations.keySet().size();
		int skip = R.nextInt(size);

		Iterator<String> it = locations.keySet().iterator();
		for (int i = 0; i < skip; i++) {
			it.next();
		}

		String host = it.next();
		
		return getHost(host);
	}
	
	public LCChunkHost getAnyHost(LCChunkHost avoid_host) {
		if (avoid_host == null) {
			return getAnyHost();
		}

		List<LCChunkHost> nds = new ArrayList<LCChunkHost>();
		nds.addAll(this.locations.values());
		nds.remove(avoid_host);

		if (nds.size() == 0) {
			LOG.warn("Request to avoid host " + avoid_host + " unsatisfiable -"
					+ "- only one host for chunk " + this.getId());
			return getAnyHost();
		}
		return nds.get(R.nextInt(nds.size()));
	}
	public Collection<LCChunkHost> getHosts() {
		return locations.values();
	}

	public Collection<String> getLocations() {
		return locations.keySet();
	}

	public String toString() {
		return this.id;
	}
}
