import java.io.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

//find the max salary drawn by each group 1.<=20 2.>20&<=30 3.>30
public class Partioner_Age extends Configured implements Tool {
	public static class mapper_class extends Mapper<LongWritable,Text,Text,Text>
	{
		public void map(LongWritable key,Text value, Context con) throws IOException, InterruptedException
		{
			String[] rec=value.toString().split(",");
			String gender=rec[3];
			con.write(new Text(gender), new Text(value));
		}		
	}

	public static class Age_Partioner extends Partitioner <Text,Text>
	{
		@Override
		public int getPartition(Text key, Text value, int numReduceTasks) {
			String[] rec=value.toString().split(",");
			int age=Integer.parseInt(rec[2]);
			if(age<=20)
			{						
				return 0;
			}
			else if(age>20&&age<=30)
			{
				return 1;
			}
			else
			{
				return 2;
			}
		}		
	}

	public static class reducer_class extends Reducer<Text, Text, Text,IntWritable>
	{
	public void reduce(Text key,Iterable<Text> value,Context con) throws IOException, InterruptedException
	{
		int max=0;
		for(Text val:value)
		{
			String[] rec=val.toString().split(",");
			if(Integer.parseInt(rec[4])>max)
			{
				max=Integer.parseInt(rec[4]);
			}
		}con.write(key, new IntWritable(max));
	}
}

	public int run(String[] args) throws IOException, ClassNotFoundException, InterruptedException
	{
		Configuration conf=new Configuration();
		Job job=Job.getInstance(conf,"");
		job.setJarByClass(Partioner_Age.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setMapperClass(mapper_class.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);		
		job.setPartitionerClass(Age_Partioner.class);
		job.setReducerClass(reducer_class.class);
		job.setNumReduceTasks(3);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		System.exit(job.waitForCompletion(true)? 0 : 1);
		return 0;
	}
	public static void main(String ar[]) throws Exception
	{
		ToolRunner.run(new Configuration(), new Partioner_Age(), ar);
		System.exit(0);
	}
}
