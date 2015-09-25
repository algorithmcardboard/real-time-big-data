import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TokenizerMapper extends Mapper<Object, Text, Text, Text> {

	private Text target = new Text();
	private Text targetValue = new Text();

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		String[] tokens = line.split("\\s+");
		System.out.println("Line:" + line);
		StringBuffer sb = new StringBuffer();
		String prValue = tokens[tokens.length - 1];
		System.out.println("Token length:" + tokens.length);
		for (int i = 1; i < tokens.length - 1; i++) {
			sb.append(tokens[i] + " ");
			System.out.println(">>>>>>>>>>>>>>" + tokens[i]);
			Double newPR = Double.parseDouble(prValue) / (tokens.length - 2);
			target.set(tokens[i]);
			targetValue.set(tokens[0] + "," + newPR.toString());
			context.write(target, targetValue);
		}
		target.set(tokens[0]);
		targetValue.set(sb.toString());
		context.write(target, targetValue);
	}
}

