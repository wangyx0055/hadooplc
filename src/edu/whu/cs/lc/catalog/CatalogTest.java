package edu.whu.cs.lc.catalog;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;

import javax.xml.bind.JAXBException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import edu.whu.cs.hadoopdb.catalog.xml.ConfigurationMapping;
import edu.whu.cs.hadoopdb.catalog.xml.Node;

public class CatalogTest {
private ConfigurationMapping xmlConfig;
	
	public static void main(String args[]) throws IOException {
		try {
			new CatalogTest("HadoopDB.xml").show();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (JAXBException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public CatalogTest(String path) throws JAXBException, IOException {
		Configuration conf = new Configuration();
		
		FileSystem fs = FileSystem.get(URI.create("hdfs://localhost:9999/user"),conf);
		
		
		xmlConfig = ConfigurationMapping.getInstance(fs.open(new Path(path)));
	}
	
	public void show() {
		Map<String,List<Node>> chunkHostMap = xmlConfig.getPartitionsForRelation("grep");
		for(String chunk_id : chunkHostMap.keySet()) {
			System.out.println(chunk_id);
			for(Node node : chunkHostMap.get(chunk_id)) {
				System.out.println(node.getRelations().toString());
				System.out.println(node.getPath());
				System.out.println(node.getAnalyzer());
				System.out.println(node.getVersion());
			}
		}
	}
}
