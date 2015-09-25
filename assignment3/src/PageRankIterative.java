import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PageRankIterative {

	public static void main(String[] args) throws Exception {
		Path input = new Path(args[0]);
		Path output;
		int i = 0;
		while (i < 3) {
			output = new Path(args[1] + i);
			Configuration conf = new Configuration();
			Job job = Job.getInstance(conf, "page rank");
			job.setJarByClass(PageRankIterative.class);
			job.setMapperClass(TokenizerMapper.class);
			job.setCombinerClass(PRSumReducer.class);
			job.setReducerClass(PRSumReducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			FileInputFormat.addInputPath(job, input);
			FileOutputFormat.setOutputPath(job, output);
			job.waitForCompletion(true);

			i++;
			input = output;
		}
	}
}
