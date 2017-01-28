import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

public class LanguageModel {
	public static class Map extends Mapper<LongWritable, Text, Text, Text> {

		int threshold;

		@Override
		public void setup(Context context) {
			//get the threshold parameter from the configuration
			Configuration configuration = context.getConfiguration();
			//default threshold is 20
			threshold = configuration.getInt("threshold", 20);
		}

		//input: I love big data\t10   //\t用来分隔key和value（\t是默认的
        //output: key: I love big, value: data=10
		
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			if((value == null) || (value.toString().trim()).length() == 0) {
				return;
			}
			//this is cool\t20
			String line = value.toString().trim();

			//split key and value by \t
            //左边就变成了this is cool, 在wordsPlusCount[0]的位置, 右边就是20, 在wordsPlusCount[1]的位置
			String[] wordsPlusCount = line.split("\t");
			//预防一些bad input
			if (wordsPlusCount.length < 2) {
				return;
			}
			//拆分左边的单词
            //{this, is, cool}
			String[] words = wordsPlusCount[0].split("\\s+");
			//get the value
			int count = Integer.valueOf(wordsPlusCount[1]);

			//filter the n-gram lower than threashold
			if (count < threshold) return;

			//this is --> cool = 20
            StringBuilder sb = new StringBuilder();
            //前面的单词都粘在一起, 最后一个单词和count粘在一起
            for (int i = 0; i < words.length - 1; ++i) {
                sb.append(words[i]);
                sb.append(" ");
            }

            //output key, 去除掉最后的空格
            String outputKey = sb.toString().trim();
			//output value
			String outputValue = words[words.length - 1] + "=" + count;

			//write key-value to reducer
            context.write(new Text(outputKey), new Text(outputValue));
		}
	}

	public static class Reduce extends Reducer<Text, Text, DBOutputWritable, NullWritable> {

		int topK;
		// get the n parameter from the configuration
        // topK用来定义需要返回几个结果
		@Override
		public void setup(Context context) {
			Configuration conf = context.getConfiguration();
            topK = conf.getInt("topK", 5);
		}

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			
			//can you use priorityQueue to rank topN n-gram, then write out to hdfs?

            //input: key: this is, value: <data=10, cool=20, ...>
            //value是iterable的


//            PriorityQueue<Pair> pq = new PriorityQueue<Pair>(topK, new Comparator<Pair>() {
//                public int compare(Pair o1, Pair o2) {
//                    return o1.freq - o2.freq;
//                }
//            });

//            for (Text val : values) {
//                String value = val.toString().trim();
//                String word = value.split("=")[0].trim();
//                int count = Integer.parseInt(value.split("=")[1].trim());
//                if (pq.contains(count)) {
//                    pq.poll(count)
//                }
//                Pair newPair = new Pair(count, word);
//            }

            //input: key: this is, value: <data=10, cool=20, ...>
            //value是iterable的
            TreeMap<Integer, List<String>> map = new TreeMap<Integer, List<String>>(Collections.<Integer>reverseOrder());

            //iterate the Text Object to get key and value
            for (Text val : values) {
                String value = val.toString().trim();
                String word = value.split("=")[0].trim();
                int count = Integer.parseInt(value.split("=")[1].trim());

                if (map.containsKey(count)) {
                    //get this list of this count
                    map.get(count).add(word);
                } else {
                    //we do not have this count
                    //make a new list to store this list of words
                    List<String> list = new ArrayList<String>();
                    list.add(word);
                    map.put(count, list);
                }

                Iterator<Integer> iter = map.keySet().iterator();
                for (int j = 0; iter.hasNext() && j < topK; ) {
                    int keyCount = iter.next();
                    List<String> words = map.get(keyCount);
                    for (int i = 0; i < words.size() && j < topK; ++i) {
                        //用DBOutputWritable写出
                        //因为我们不需要写出value, 所以用NullWritable写出value
                        context.write(new DBOutputWritable(key.toString(), words.get(i), keyCount), NullWritable.get());
                        j++;
                    }
                }
            }


		}
//		public class Pair {
//		    public int freq;
//		    public List<String> list;
//		    public Pair(int freq, List<String> list){
//		        this.freq = freq;
//		        this.list = list;
//            }
//        }
	}
}
