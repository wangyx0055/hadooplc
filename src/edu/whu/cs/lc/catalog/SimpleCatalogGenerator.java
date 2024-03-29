package edu.whu.cs.lc.catalog;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;

import edu.whu.cs.hadoopdb.catalog.xml.Configuration;
import edu.whu.cs.hadoopdb.catalog.xml.Node;
import edu.whu.cs.hadoopdb.catalog.xml.ObjectFactory;
import edu.whu.cs.hadoopdb.catalog.xml.Partition;
import edu.whu.cs.hadoopdb.catalog.xml.Relation;

public class SimpleCatalogGenerator {
	protected static final String NODES_FILE = "nodes_file";
	protected static final String CATALOG_FILE = "catalog_file";
	protected static final String RELATIONS_CHUNKED = "relations_chunked";
	protected static final String RELATIONS_UNCHUNKED = "relations_unchunked";
	protected static final String CHUNKS_PER_NODE = "chunks_per_node";
	protected static final String CHUNKED_DB_PREFIX = "chunked_db_prefix";
	protected static final String UNCHUNKED_DB_PREFIX = "unchunked_db_prefix";
	protected static final String PATH = "path";
	protected static final String VERSION = "version";
	protected static final String ANALYZER = "analyzer";
	protected Properties properties = new Properties();
	
	protected List<String> relations_unchunked = new ArrayList<String>();
	protected List<String> relations_chunked = new ArrayList<String>();
	protected List<String> nodes = new ArrayList<String>();
	
	public static void main(String args[]) throws FileNotFoundException, IOException {
		SimpleCatalogGenerator catgen = new SimpleCatalogGenerator();
		catgen.parseArguments(args);	
		try {
			catgen.generateCatalog();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (JAXBException e) {
			e.printStackTrace();
		}
		
	}
	
	protected void parseArguments(String[] args) throws FileNotFoundException, IOException {
		properties.load(new FileInputStream(new File(args[0])));
		relations_chunked = Arrays.asList(properties.getProperty(RELATIONS_CHUNKED).split("\\s*,\\s*"));
		relations_unchunked = Arrays.asList(properties.getProperty(RELATIONS_UNCHUNKED).split("\\s*,\\s*"));
		nodes = getStringsAsList(new File(properties.getProperty(NODES_FILE)));
	}
	
	protected static List<String> getStringsAsList(File rfile) {
		List<String> list = new ArrayList<String>();
		try {
			BufferedReader br = new BufferedReader(new FileReader(rfile));
			String str = "";
			while((str = br.readLine()) != null) {
				list.add(str.trim());
			}
		}catch(FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return list;
	}
	
	public void generateCatalog() throws JAXBException, FileNotFoundException {
		JAXBContext jc = JAXBContext.newInstance("edu.whu.cs.hadoopdb.catalog.xml");
		Marshaller marshaller = jc.createMarshaller();
		marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT,new Boolean(true));
		
		ObjectFactory factory = new ObjectFactory();
		Configuration configuration = factory.createConfiguration();
		
		int node_counter = 0;
		for(String node : nodes){
			Node n = factory.createNode();
			n.setPath(properties.getProperty(PATH));
			n.setAnalyzer(properties.getProperty(ANALYZER));
			n.setVersion(properties.getProperty(VERSION));
			n.setLocation(node);

			for(String relation : relations_unchunked){
				Relation r = factory.createRelation();
				r.setId(relation);
				Partition p = factory.createPartition();
				p.setId(new Integer(node_counter).toString());
				p.setUrl(properties.getProperty(PATH));
				r.getPartitions().add(p);
				n.getRelations().add(r);
			}
			
			
			for(String relation : relations_chunked){
				Relation r = factory.createRelation();
				r.setId(relation);
				
				int start_index = node_counter*(new Integer(properties.getProperty(CHUNKS_PER_NODE)));
				for(int index = start_index; index < start_index + (new Integer(properties.getProperty(CHUNKS_PER_NODE))); index ++) {
					Partition p = factory.createPartition();
					p.setId(new Integer(index).toString());
					p.setUrl(properties.getProperty(PATH));
					r.getPartitions().add(p);
				}
				n.getRelations().add(r);
			}			
			
			node_counter++;
			configuration.getNodes().add(n);
		}
		JAXBElement<Configuration> jaxb = factory.createLCClusterConfiguration(configuration);
		marshaller.marshal(jaxb, new FileOutputStream(new File(properties.getProperty(CATALOG_FILE))));
	}
	
}
