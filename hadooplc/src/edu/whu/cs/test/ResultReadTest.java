package edu.whu.cs.test;


import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;

public class ResultReadTest {

	public static void main(String[] args ) throws Exception {
		FloatWritable score = new FloatWritable();
		MapWritable valueMap = new MapWritable() ;
		Configuration conf = new Configuration();
		conf.set("fs.default.name", "hdfs://127.0.0.1:8888") ;
		System.out.println(conf.get("fs.default.name") ) ;
		FileSystem fs = FileSystem.get(conf) ;
		Path path  = new Path("/user/cscyl/outdir/part-00000") ;
		
		SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, conf) ;
		FloatWritable key = (FloatWritable)ReflectionUtils.newInstance(reader.getKeyClass(), conf) ;
		MapWritable value = (MapWritable)ReflectionUtils.newInstance(reader.getValueClass(), conf) ;
		//MapWritable value = new MapWritable();
		while( reader.next(key,value) ) {
			System.out.println();
			System.out.print(key + ":[ " ) ;
			Iterator<Writable> sit = value.keySet().iterator() ;
			while( sit.hasNext() ) {
				Text k = (Text)sit.next() ;
				Text v = (Text)value.get(k) ;
				System.out.print(k + ": " +v ) ; 
			}
			System.out.print("]") ;
		}
	}
}
