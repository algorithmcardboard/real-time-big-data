import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper extends Mapper<Object, Text, Text, IntWritable> {

	private final static IntWritable zero = new IntWritable(0);
	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

		Configuration conf = context.getConfiguration();

		String tokens[] = conf.getStrings("wordsToCheck");
		String line = value.toString().toLowerCase();

		for (String token : tokens) {
			word.set(token);
			if (line.contains(token)) {
				context.write(word, one);
			} else {
				context.write(word, zero);
			}
		}
	}
}
