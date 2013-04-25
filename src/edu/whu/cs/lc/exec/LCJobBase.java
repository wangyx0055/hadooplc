/**
 * @author chenbiren
 * @file LCJobBase.java
 * @date 2012-9-24
 **/
package edu.whu.cs.lc.exec;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import edu.whu.cs.lc.connector.LCConst;
import edu.whu.cs.lc.connector.LCInputFormat;
import edu.whu.cs.lc.connector.LCWritable;

public abstract class LCJobBase extends Configured implements Tool {
	public static final Log LOG = LogFactory.getLog(LCJobBase.class.getName());
	
	protected abstract JobConf configureJob(String... args) throws Exception;
	
	protected JobConf initConf(String[] args) throws Exception {
		List<String> other_args = new ArrayList<String>();

		Path configuration_file = null;
		boolean replication = false;
		

		for (int i = 0; i < args.length; ++i) {
			
			if (("-"+LCConst.LC_CONFIG_FILE).equals(args[i])) {
				System.out.println(LCConst.LC_CONFIG_FILE + ": " + args[i + 1]);
				configuration_file = new Path(args[++i]);
			} else if ("-replication".equals(args[i])) {
				replication = true;
			} else {
				other_args.add(args[i]);
			}
		}
		
		JobConf conf = null;

		conf = configureJob(other_args.toArray(new String[0]));
		LOG.info(conf.getJobName());
		LOG.info(conf.get(LCConst.LC_QUERY));
		if(conf.get(LCConst.LC_RELATION_ID) == null || conf.get(LCConst.LC_QUERY) == null
				|| conf.get(LCConst.LC_RECORD_READER) == null) {
			throw new Exception(
				"ERROR: Job require a relation, an Query and a Doc Reader"
			);
		}
		if(replication) {
			conf.setBoolean(LCConst.LC_REPLICATION, true);
		}
		if(configuration_file == null) {
			if(conf.get(LCConst.LC_CONFIG_FILE) == null) {
				throw new Exception("No HadoopLC config file");
			}
		}
		else {
			conf.set(LCConst.LC_CONFIG_FILE, configuration_file.toString());
		}
		setInputFormat(conf);
		return conf;
	}
	
	protected void setInputFormat(JobConf conf) {
		conf.setInputFormat(LCJobBaseInputFormat.class);
	}
	
	
	protected abstract int printUsage();
	public int printLcUsage() { 
		printUsage();
		ToolRunner.printGenericCommandUsage(System.out);
		return -1;
	}
	
	protected static class LCJobBaseInputFormat extends LCInputFormat<LCWritable> {
		@Override
		public void configure(JobConf conf) {
			super.configure(conf);
			long startTime = System.currentTimeMillis();
			lcConf.setLcQuery(conf.get(LCConst.LC_QUERY));
			conf.setInt(LCConst.LC_FETCH_SIZE, conf.getInt(LCConst.LC_FETCH_SIZE,
					LCConst.LC_DEFAULT_FETCH_SIZE));
			try {
				lcConf.setValueClass(Class.forName(conf.get(LCConst.LC_RECORD_READER)));
//				lcConf.setValueClass(Class.forName(edu.whu.cs.lc.benchmark.GrepTaskLC.DocumentRecord.class.getName()));
			} catch (ClassNotFoundException e) {
				LOG.error("No RecordReader class specified.", e);
			}			

			long endTime = System.currentTimeMillis();
			LOG.debug(LCJobBaseInputFormat.class.getName() + ".configure() time (ms): "
					+ (endTime - startTime));
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		long startTime = System.currentTimeMillis();
		JobConf conf = null;
		try {
			conf = initConf(args);
		} catch(Exception e) {
			System.err.print("ERROR: " + StringUtils.stringifyException(e));
			return printLcUsage();
		}
		
		JobClient.runJob(conf);
		long endTime = System.currentTimeMillis();
		LOG.info("\n" + conf.getJobName() + " JOB TIME : " + (endTime - startTime) + " ms.\n");
		return 0; 
	}
}