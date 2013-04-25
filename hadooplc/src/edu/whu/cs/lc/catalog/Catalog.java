/**
 * Copyright 2009 HadoopDB Team (http://db.cs.yale.edu/hadoopdb/hadoopdb.html)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package edu.whu.cs.lc.catalog;


import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.xml.bind.JAXBException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.StringUtils;

import edu.whu.cs.hadoopdb.catalog.xml.*;
import edu.whu.cs.lc.connector.LCChunk;
import edu.whu.cs.lc.connector.LCChunkHost;
import edu.whu.cs.lc.connector.LCConst;

/**
 * HadoopDB Catalog: Simple XML file based implementation. No support for mid-job updates
 * except by recreating the XML catalog file and re-executing the job. 
 */
public class Catalog {

	public static final Log LOG = LogFactory.getLog(Catalog.class.getName());
	
	
	private static Catalog singleton;

	public static Catalog getInstance(JobConf job) {
		if (singleton == null)
			singleton = new Catalog(job);
		return singleton;
	}

	private ConfigurationMapping xmlConfig;
	private boolean replication = false;

	private Catalog(JobConf job) {

		try {
			FileSystem fs = FileSystem.get(URI.create("/") , job ) ;
			System.out.println(job.get("fs.default.name"));
			System.out.println(fs.getDefaultUri(job));
			
			LOG.info(job.get(LCConst.LC_CONFIG_FILE));
			Path config_file = new Path(job.get(LCConst.LC_CONFIG_FILE));
			xmlConfig = ConfigurationMapping.getInstance(fs.open(config_file));
			replication = job.getBoolean(LCConst.LC_REPLICATION, false);
		} catch (IOException e) {
			LOG.error(StringUtils.stringifyException(e));
		} catch (JAXBException e) {
			LOG.error(StringUtils.stringifyException(e));
		}
	}
	
	/**
	 * For a given relation, it populates the configuration object with the different
	 * chunks associated with the relation. Each chunk contains connection and location
	 * information.
	 *  
	 */
	public void setSplitLocationStructure(BaseLCConfiguration lcConf, String relation) {
		
		lcConf.setRelation(relation);
		for(LCChunk chunk : getSplitLocationStructure(relation)) {
			lcConf.addChunk(chunk);
		}
	}	

	/**
	 * For a given relation, it returns a collection of chunks associated with the relation. 
	 * Each chunk contains connection and location information.
	 */
	public Collection<LCChunk> getSplitLocationStructure(String relation) {
		
		Collection<LCChunk> list = new ArrayList<LCChunk>();

		Map<String, List<Node>> chunkHostMap = xmlConfig
				.getPartitionsForRelation(relation);
		if(chunkHostMap == null) {
			throw new RuntimeException("Relation '" + relation + "' is not defined in the catalog.");
		}
		Set<Node> usedNodes = new HashSet<Node>();
		for (String chunk_id : chunkHostMap.keySet()) {
			LCChunk chunk = new LCChunk(chunk_id);
			if (replication) {
				for (Node node : chunkHostMap.get(chunk_id)) {
					chunk.addHost(new LCChunkHost(node.getLocation(),
							xmlConfig.getPartitionForNodeRelation(node,	relation, chunk_id).getUrl(), 
							node.getPath(), 
							node.getAnalyzer(), 
							node.getVersion()));
				}
			} else {
				for (Node node : chunkHostMap.get(chunk_id)) {
					if (usedNodes.contains(node))
						;
					else {
						usedNodes.add(node);
						chunk.addHost(new LCChunkHost(node.getLocation(),
								xmlConfig.getPartitionForNodeRelation(node,relation, chunk_id).getUrl(),
								node.getPath(), 
								node.getAnalyzer(), 
								node.getVersion()));
						break;

					}
				}
				if (chunk.getHosts().isEmpty()) {
					Node node = chunkHostMap.get(chunk_id).get(0);
					chunk.addHost(new LCChunkHost(node.getLocation(),
							xmlConfig.getPartitionForNodeRelation(node,relation, chunk_id).getUrl(), 
							node.getPath(), 
							node.getAnalyzer(), 
							node.getVersion()));
				}
			}
			list.add(chunk);
		}
		
		return list;
	}
		
	
}
