package com.fit.dba.mr.util;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.ReflectionUtils;
import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.File;

public class SequenceFileValueExtractor {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Path path = new Path(args[0]);
		String outPath= args[1];
		
		SequenceFile.Reader reader = null;
		try {
			reader = new SequenceFile.Reader(conf, Reader.file(path));
			Text key = new Text();
			//BytesWritable value = (BytesWritable)ReflectionUtils.newInstance(reader.getValueClass(), conf);
			BytesWritable value = new BytesWritable();
			
			while (reader.next(key,value)) { 
				byte[] ba = value.copyBytes();
				String imageName = key.toString().substring(key.toString().lastIndexOf('/')+1);
				BufferedImage bImageFromConvert = ImageIO.read(new ByteArrayInputStream(ba));
				ImageIO.write(bImageFromConvert, "png", new File(outPath+"/"+imageName));
				
				System.out.println(key);
			
			}
		} finally {
			IOUtils.closeStream(reader);
		}
	}
}
