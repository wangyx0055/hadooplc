import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;


public class HdfsTest {
	
	public static void main (String[] args ) throws IOException {
		Configuration conf = new Configuration() ;
		FileSystem fs = FileSystem.get(URI.create("hdfs://localhost:9999"), conf) ;
		
	}
	
}