package edu.whu.cs.lc.connector;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.queryParser.MultiFieldQueryParser;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocsCollector;
import org.apache.lucene.search.TopScoreDocCollector;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.lucene.util.Version;

import com.chenlb.mmseg4j.analysis.SimpleAnalyzer;

public class MyLuceneTestCase {

	public static final Log LOG = LogFactory.getLog(DocumentRecord.class.getName());
	
	private String key="";
	
	private String value="";
	
	private Directory dir;
	
	private IndexWriterConfig writerConfig;
	
	private IndexReader reader;
	
	private IndexSearcher searcher;
	
	private Analyzer analyzer;
	
	private IndexWriter writer;
	
	TopScoreDocCollector results = TopScoreDocCollector.create(100, false);
	
	ResultSet hits;
	
	public MyLuceneTestCase(String key ,String value) {
		this.key = key;
		this.value = value;
		analyzer = new SimpleAnalyzer();
		writerConfig = new IndexWriterConfig(    
                Version.LUCENE_36, analyzer); 
	}
	
	public void open(String path) {
		try {
			dir = FSDirectory.open(new File(path));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void write(int num) throws CorruptIndexException, LockObtainFailedException, IOException {
		open("/home/cscyl/hadoopdb/index");
		writer = new IndexWriter(dir,writerConfig);
		
		for(int i = 0; i < num; i++) {
			Document doc = new Document();
			doc.add(new Field("key", key + i, Store.YES, Index.ANALYZED));
			doc.add(new Field("value", value + i, Store.YES, Index.ANALYZED));
			writer.addDocument(doc);
		}
		
		writer.close();
		dir.close();
	}
	
	public void search() throws CorruptIndexException, IOException, ParseException {
		open("/home/cscyl/hadoopdb/index");
		reader = reader.open(dir);
		searcher = new IndexSearcher(reader);
		QueryParser qp = new QueryParser(Version.LUCENE_36,"name",analyzer);
		
		Query query = qp.parse("聚烯烃");
		
		
		searcher.search(query, results); 
		
		hits = new ResultSet(results,searcher);
	}
	
	public void show() throws CorruptIndexException, IOException {
		int n = results.getTotalHits();
		
		System.out.println("total results: " + n);
		LOG.info(results.getTotalHits() ) ;
		ScoreDoc[] docs = results.topDocs().scoreDocs;
		System.out.println("Docs length: " + docs.length);
		for(int i = 0; i < docs.length; i++) {
			Document doc = searcher.doc(docs[i].doc);
			System.out.println("name:" + doc.get("name"));
			System.out.println("unit:" + doc.get("unit"));
			
		}
		System.out.println("total hits: " + hits.getTotalHits());
		while(hits.next()) {
			System.out.println(hits.getKey());
			Map<Text,Text> value = (HashMap<Text,Text>)hits.getValue() ;
			Iterator<Text> sit = value.keySet().iterator() ;
			while( sit.hasNext() ) {
				String k = sit.next().toString() ;
				LOG.info("key:"+ k + " value:" + value.get(k)) ;
			}
		}
	}
	
	public static void main(String args[]) throws CorruptIndexException, LockObtainFailedException, IOException, ParseException {
			
			MyLuceneTestCase test = new MyLuceneTestCase("key","value");
			//test.write(Integer.parseInt(args[2]));
			
			test.search();
			
			test.show();
	}
}
