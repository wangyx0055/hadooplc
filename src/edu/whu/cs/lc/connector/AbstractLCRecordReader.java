/**
 * @author chenbiren
 * @file AbstractLuceneReader.java
 * @date 2012-9-24
 **/
package edu.whu.cs.lc.connector;

import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.queryParser.MultiFieldQueryParser;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopDocsCollector;
import org.apache.lucene.search.TopScoreDocCollector;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;

import com.chenlb.mmseg4j.analysis.SimpleAnalyzer;

public abstract class AbstractLCRecordReader {
	public static final Log LOG = LogFactory.getLog(AbstractLCRecordReader.class.getName());
	
	public static final int MAX_CONNECTION_TRIALS = 10;
	
	protected ResultSet results;
	private Directory dir;
	private IndexReader reader;
	private IndexSearcher searcher;
	private Analyzer analyzer;
	private TopDocsCollector<ScoreDoc> collectors;
	protected long pos = 0;
	
	protected long startTime = 0;
	protected long connTime = 0;
	protected long queryTime = 0;
	protected long endTime = 0;
	
	private static String getLocatHostAddres() {
		try {
		//	return InetAddress.getLocalHost().getCanonicalHostName();
			return InetAddress.getLocalHost().getHostName();
		} catch (UnknownHostException e) {
			return null;
		}
	}

	public abstract String getLuceneQuery();

	protected void setupLC(LCInputSplit split,JobConf conf) throws IOException, ParseException{
		startTime = System.currentTimeMillis();
		dir = initSetup(split,conf);
		reader = reader.open(dir);
		searcher = new IndexSearcher(reader);
		connTime = System.currentTimeMillis();
		SimpleAnalyzer spAnalyzer = new SimpleAnalyzer() ;
		String querystr = conf.get(LCConst.LC_QUERY) ;
		QueryParser qp = new QueryParser(Version.LUCENE_36,"name",new StandardAnalyzer(Version.LUCENE_36));
		LOG.info(querystr) ;
		Query query = qp.parse(querystr);
		collectors = TopScoreDocCollector.create(conf.getInt(LCConst.LC_FETCH_SIZE,LCConst.LC_DEFAULT_FETCH_SIZE), true);
		searcher.search(query, collectors);
		results = new ResultSet(collectors,searcher);
		queryTime = System.currentTimeMillis();
	}
	
	protected String prepareLcQuery(String lcQuery,LCInputSplit split,JobConf conf) {
		String prepareClass = conf.get(LCConst.LC_PREPARER);
		if(prepareClass == null) {
			return lcQuery;
		} else {
			try {
				LCPreparer lcPreparer = (LCPreparer) ReflectionUtils
						.newInstance(Class.forName(prepareClass), conf);
				return lcPreparer.prepare(lcQuery, split, conf);
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}
	}
	
	protected Directory initSetup(LCInputSplit lcSplit,JobConf job) throws IOException {
		boolean connected = false;
		LCChunkHost avoid_host = null;
		int connect_tries = 0;
		Directory connection = null;

		String localHostAddr = getLocatHostAddres();
		LCChunk chunk = lcSplit.getChunk();
//		Collection<String> hosts = chunk.getLocations();
//		Iterator<String> it = hosts.iterator();
//		while(it.hasNext()) {
//			System.out.println(it.next());
//		}
		LCChunkHost chunk_host = null;
		LOG.info("Run Place: " + localHostAddr);
		while (!connected) {
			if (!chunk.getLocations().contains(localHostAddr)) {
				LOG.info("Data locality failed for " + localHostAddr);
				chunk_host = chunk.getAnyHost(avoid_host);
			} else {
				if (avoid_host != chunk.getHost(localHostAddr)) {
					chunk_host = chunk.getHost(localHostAddr);
				} else {
					chunk_host = chunk.getAnyHost(avoid_host);
				}
			}
			LOG.info("Task from " + localHostAddr + " is connecting to chunk "
					+ chunk.getId() + " on host " + chunk_host.getHost()
					+ " with index url " + chunk_host.getUrl());

			try {
				/*Class lcAnalyzer = Class.forName(chunk_host.getAnalyzer());
				Constructor con = lcAnalyzer.getConstructor(Version.LUCENE_36.getClass());
				analyzer = (Analyzer) con.newInstance(Version.LUCENE_36);*/
				connection = FSDirectory.open(new File(chunk_host.getUrl()));
				connected = true;
			} catch (Exception e) {
				LOG.info("An error to open the index. See below for details.");
				LOG.info(e);
				if (connect_tries < MAX_CONNECTION_TRIALS) {
					connect_tries++;
					avoid_host = chunk_host;
					chunk_host = null;
				} else
					throw new RuntimeException(e);
			}
		}
		return connection;
	}
	//RecordReader
	public void close() throws IOException {
		searcher.close();
		reader.close();
		dir.close();
		endTime = System.currentTimeMillis();
		LOG.info("Time(ms) : open = " + (connTime - startTime) 
				+ ",Query = " + (queryTime - connTime)
				+ ",Totle = " + (endTime - startTime));
	}
	//RecordReader
	public FloatWritable createKey() {
		return new FloatWritable();
	}
	//RecordReader
	public long getPos() throws IOException {
		return pos;
	}
	//RecordReader
	public float getProgress() throws IOException {
		return 0;
	}
}
