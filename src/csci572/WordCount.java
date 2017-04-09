package csci572;

import java.io.IOException;
import java.util.HashMap;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {

	static String match = "warning";
	public static class TokenizerMapper
	extends Mapper<Object, Text, Text, Text>{

		private Text word = new Text();

		public void map(Object key, Text value, Context context
				) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			Text docID = new Text(itr.nextToken() + ":1");
			while (itr.hasMoreTokens()) {
				String s = itr.nextToken();
				word.set(s);
				if(s.equalsIgnoreCase(match)){
					System.out.println(s);
				}
				context.write(word, docID);
			}
		}
	}

	public static class IntSumReducer
	extends Reducer<Text,Text,Text,Text> {
		private Text result = new Text();

		public void reduce(Text key, Iterable<Text> values,
				Context context
				) throws IOException, InterruptedException {
			StringBuffer docs = new StringBuffer();
			HashMap<String, Integer> count = new HashMap<>();

			for (Text docIDColonFormat : values) {
				String[] docIDCount = docIDColonFormat.toString().split(":");
				if(docIDCount.length !=2){
					throw new IllegalArgumentException();
				}

				String docID = docIDCount[0];
				Integer c = Integer.parseInt(docIDCount[1].trim());
				if(count.get(docID)!=null){
					count.put(docID, count.get(docID)+c);
				}else{
					count.put(docID, c);
				}
			}

			for(String docID : count.keySet()){
				docs.append(docID).append(":").append(count.get(docID)).append(" ");
				//System.out.println(docs);
			}
			//if(key.toString().equalsIgnoreCase(match)){
				System.out.println(key + " -> " + docs);
			//}
			
			// key docID:count docID:count
			result.set(docs.toString());
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "word count");
		job.setJarByClass(WordCount.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}