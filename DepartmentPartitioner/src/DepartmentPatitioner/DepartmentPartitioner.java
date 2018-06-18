package DepartmentPatitioner;


import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class DepartmentPartitioner {

	public static class DepartmentMapper extends Mapper<Object,Text,Text,Text>{
		Text department = new Text();
		Text rest_vals  = new Text();
		public void map(Object key,Text value,Context context) throws IOException, InterruptedException{
			String[] parts = value.toString().split(",");
			department.set(parts[2]);
			String all = parts[0]+"\t"+parts[1]+"\t"+parts[3]+"\t"+parts[4];
			rest_vals.set(all);
			context.write(department, rest_vals);
			
		}
	}
	public static class DepartmentReducer extends Reducer<Text,Text,Text,Text>{
		long highest_sal = Long.MIN_VALUE;
		String emp_name = "";
		String emp_desgn = "";
		String emp_id = "";
		public void reduce(Text key,Iterable<Text> values,Context context) throws IOException, InterruptedException{
			for (Text val : values){
				String[] breaks = val.toString().split("\t");
				long emp_sal = Long.parseLong(breaks[3]);
				if(emp_sal > highest_sal){
					highest_sal = emp_sal;
					emp_id = breaks[0];
					emp_name = breaks[1];
					emp_desgn = breaks[2];
				}	
			}
			String all_vals = emp_id+"\t"+emp_name+"\t"+emp_desgn+"\t"+highest_sal;
			context.write(key, new Text(all_vals));
		}
	}
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf,"Partitioner Example");
		job.setJarByClass(DepartmentPartitioner.class);
		job.setMapperClass(DepartmentMapper.class);
		job.setReducerClass(DepartmentReducer.class);
		job.setNumReduceTasks(3);
		job.setPartitionerClass(CustomDepartmentPartitioner.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		Path outputPath = new Path(args[1]);
		FileOutputFormat.setOutputPath(job, outputPath);
		outputPath.getFileSystem(conf).delete(outputPath, true);
		System.exit(job.waitForCompletion(true)? 0 : 1);
		
	}
}
