package edu.whu.cs.lc.connector;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocsCollector;

public class ResultSet {
	public static final Log LOG = LogFactory.getLog(DocumentRecord.class.getName());
	
	private TopDocsCollector<ScoreDoc> collectors;
    
	private IndexSearcher searcher;
    
	private ScoreDoc[] docs;
	
	private int index = -1;
	
	private int totalHits;
	
    public ResultSet(TopDocsCollector<ScoreDoc> collectors,IndexSearcher searcher) {
    	this.collectors = collectors;
    	this.searcher = searcher;
    	totalHits = collectors.getTotalHits();
    	docs = collectors.topDocs().scoreDocs;
    }
    
    public boolean next() {
    	if(index < docs.length - 1) {
    		index++;
	    	return true;
    	} else
    		return false;
    }
    
    public int getTotalHits() {
    	return totalHits;
    }
    public float getKey(){
    	return docs[index].score ;
    }
    
    public Map<Text,Text> getValue() {
    	Document doc = null;
    	Map<Text,Text> resultMap = new HashMap<Text,Text> () ;
    	try {
			doc = searcher.doc(docs[index].doc);
	    	String[] keys = {"name","unit","result_level","channel","category","period","description"} ;
	    	for ( String key : keys ) {
	    		LOG.info("key:" + key ) ;
	    		Text tk = new Text(key) ;
	    		Text tv = new Text(doc.get(key) ) ;
	    		resultMap.put(new Text(key), new Text(doc.get(key)) );
	    	}
	    	
		} catch (CorruptIndexException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
    	return resultMap ;
    }

	@Override
	public String toString() {
		return "ResultSet [docs=" + Arrays.toString(docs) + ", index=" + index
				+ ", totalHits=" + totalHits + "]";
	}
    
    
}
