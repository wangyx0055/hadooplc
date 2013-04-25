/**
 * @author chenbiren
 * @file BaseLCConfiguration.java
 * @date 2012-9-25
 **/
package edu.whu.cs.lc.catalog;

import java.util.Collection;
import java.util.HashMap;

import edu.whu.cs.lc.connector.*;
public class BaseLCConfiguration {
	protected HashMap<String,LCChunk> chunks = new HashMap<String,LCChunk>();
	protected String relation;
	public BaseLCConfiguration() {
		
	}
	public void setRelation(String relation){
		  this.relation = relation;
	}
	public String getRelation(){
		  return this.relation;
	}
	
	public LCChunk getChunk(String id) {
		return chunks.get(id);
	}
	
	public void addChunk(LCChunk chunk) {
		chunks.put(chunk.getId(), chunk);
	}
	
	public Collection<LCChunk> getChunks() {
		return chunks.values();
	}
}
