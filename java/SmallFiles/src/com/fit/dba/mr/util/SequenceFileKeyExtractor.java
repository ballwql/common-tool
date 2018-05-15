package com.fit.dba.mr.util;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.Text;
 
public class SequenceFileKeyExtractor {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Path path = new Path(args[0]);
		SequenceFile.Reader reader = null;
		try {
			reader = new SequenceFile.Reader(conf, Reader.file(path));
			Text key = new Text();
			while (reader.next(key)) { System.out.println(key);
			}
		} finally {
			IOUtils.closeStream(reader);
		}
	}
}