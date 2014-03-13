/**********************************************************************************
****Reference Material:							***********
****Lecture Slides, Tutorial Material					***********
****Book: Hadoop, Beginner's Guide By: Gary Turkington, PaCKT Publishing***********
**********Chapter:5 page 129-133					***********
**********************************************************************************/

package org.apache.hadoop.examples;

import java.io.* ;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class AuthorUrl
{
//********************************Mapper Classs**************************************
	public static class AuthorUrlMapper extends Mapper<Object, Text, Text, Text>
	{
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			String row = value.toString() ;			      //Reading records and then split them with " as delimiter 
			String[] field = row.split("\"");			
			int valid=0; int count=0; int authindex=0, urlindex=0;//variables to check if record contains both author and url, & return the position index 
			for(String val : field)
			{
				if(val.contains("author"))		     //check for author
				{
					valid++; 
					authindex=count; 
				}
				if(val.contains("url"))			     //check for url
				{
					urlindex=count; 
					valid++; 
				}
				count++; 

			}
			if(valid==2)					    //if both author and url exist then  pass the author name & url
			context.write(new Text(field[authindex+2]), new Text("\""+field[urlindex+2]+"\"")) ;
		}
	}
//********************************Reducer Classs**************************************
	public static class AuthorUrlReducer extends Reducer<Text, Text, Text, Text>
	{
		public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException
		{
			int count=0;						    //count variable is used to make sure that comma dose not appear for first url				
			String urls="";
			for(Text itr : values)					   // putting url for same authors together till urls are available
			{
				if(count!=0)
					urls = String.format("%s,%s", urls,itr) ;
				else
					urls = String.format("%s", itr) ;
				count++; 
			}
			context.write(new Text(key+"\t"), new Text(urls));
		}
	}
//********************************Driver Classs**************************************

	public static void main(String[] args) throws Exception
	{
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) 
		{
			System.err.println("Usage: AuthorUrl <in> <out>");
			System.exit(2);
		}
		Job job = new Job(conf, "Author URL");
		job.setJarByClass(AuthorUrl.class);
		job.setMapperClass(AuthorUrlMapper.class);
		job.setReducerClass(AuthorUrlReducer.class);
		job.setCombinerClass(AuthorUrlReducer.class);     	//with combiner inputs to reducer are reduced to 270, compared to 303 without combiner.  
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
