import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PRSumReducer extends Reducer<Text, Text, Text, Text> {
	private Text result = new Text();

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		Double sumPR = 0.0;
		String outlinks = "";
		for (Text value : values) {
			String targetValue = value.toString();
			if (targetValue.contains(",")) {
				String[] tokens = targetValue.toString().split(",");
				sumPR += Double.parseDouble(tokens[1]);
			} else {
				outlinks = targetValue;
			}
		}
		result.set(outlinks + sumPR.toString().substring(0, sumPR.toString().length() - 2));
		context.write(key, result);
	}
}
