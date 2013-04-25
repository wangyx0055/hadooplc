/**
 * @author chenbiren
 * @file SelectTaskLC.java
 * @date 2012-10-8
 **/
package edu.whu.cs.lc.benchmark;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.lucene.search.TopDocs;

import edu.yale.cs.hadoopdb.util.HDFSUtil;
import edu.whu.cs.lc.connector.*;
import edu.whu.cs.lc.exec.LCJobBase;
public class GrepTaskLC extends LCJobBase {
	public static final String PAGE_RANK_VALUE_PARAM = "page.rank.value";
		
	public static void main(String args[]) throws Exception {
		int res = ToolRunner.run(new Configuration(),new GrepTaskLC(), args);
		System.exit(res);
	}
	
	

	@Override
	protected JobConf configureJob(String... args) throws Exception {
		JobConf conf = new JobConf(this.getClass());
		conf.setJobName("grep_lc");

		conf.setOutputKeyClass(FloatWritable.class);
		conf.setOutputValueClass(MapWritable.class);

		conf.setMapperClass(Map.class);
		conf.setNumReduceTasks(0);
		conf.setOutputFormat(SequenceFileOutputFormat.class);
		if (args.length < 2) {
			throw new RuntimeException("Incorrect arguments provided for "
					+ this.getClass());
		}
		//set default parameters
		conf.set(LCConst.LC_RELATION_ID, "grep");
		conf.set(LCConst.LC_RECORD_READER, DocumentRecord.class.getName());
		conf.set(LCConst.LC_QUERY, "key*");
		// OUTPUT properties
		for (int i = 0; i < args.length; ++i) {
			if ("-pattern".equals(args[i])) {
				System.out.println("pattern: " + args[i +1]);
				conf.set("pattern", args[++i]);
			} else if("-output".equals(args[i])) {
				conf.set("output", args[++i]);
			} else if (("-" + LCConst.LC_QUERY).equals(args[i]) ) {
				conf.set(LCConst.LC_QUERY, args[++i]) ;
			}
		}
		Path outputPath = new Path(conf.get("output"));
		//System.out.println( conf.get("output")) ;
		HDFSUtil.deletePath(outputPath);
		FileOutputFormat.setOutputPath(conf, outputPath);
		return conf;
	}

	@Override
	protected int printUsage() {
		System.out.println("<page_rank_value> <output_dir>");
		return -1;
	}

	public static class Map extends MapReduceBase implements
			Mapper<FloatWritable, DocumentRecord, FloatWritable,MapWritable> {

		@Override
		public void map(FloatWritable key, DocumentRecord value,
				OutputCollector<FloatWritable,MapWritable> output, Reporter report)
				throws IOException {
			
			output.collect(value.getKey(),value.getValue());
		}

	}
	
	
}