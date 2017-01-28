import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class NGramLibraryBuilder {
	public static class NGramMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

		int noGram;
		@Override
		public void setup(Context context) {
			//get n-gram from command line
			Configuration conf = context.getConfiguration();
			noGram = conf.getInt("noGram", 5);
		}

		// map method
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			//remove useless elements
			String line = value.toString().trim().toLowerCase().replaceAll("[^a-z]", " ");

			//separate word by space
			String[] words = line.split("\\s+");
			if (words.length < 2) {
				return;
			}

			//build n-gram based on array of words
			StringBuilder sb;
			for (int i = 0; i < words.length; ++i) {
				sb = new StringBuilder();
				sb.append(words[i]);
				for (int j = i; i + j < words.length && j < noGram; ++j) {
					sb.append(" ");
					sb.append(words[i + j]);
					context.write(new Text(sb.toString()), new IntWritable(1));
				}
			}
		}
	}

	public static class NGramReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		// reduce method
		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {

			//sum up the total count for each n-gram
			int sum = 0;
			for (IntWritable value : values) {
				sum += value.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}

}