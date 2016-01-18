package ca.uwaterloo.cs.bigdata2016w.xeniaqian94.assignment1;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ParserProperties;

import com.google.common.collect.Sets;

import tl.lin.data.pair.PairOfStrings;

/**
 * Simple word count demo.
 */
public class PairsPMI extends Configured implements Tool {
	private static final Logger LOG = Logger.getLogger(PairsPMI.class);
	private static int countLine = 0;

	// Mapper: emits (token, 1) for every word occurrence per line
	private static class FirstMapper extends Mapper<LongWritable, Text, Text, FloatWritable> {
		// Reuse objects to save overhead of object creation.
		private static final FloatWritable ONE = new FloatWritable(1);
		private final static Text WORD = new Text();

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = ((Text) value).toString();
			countLine += 1;

			StringTokenizer itr = new StringTokenizer(line);

			int cnt = 0;
			Set<String> set = new HashSet<String>();
			while (itr.hasMoreTokens()) {
				cnt++;
				String w = itr.nextToken().toLowerCase().replaceAll("(^[^a-z]+|[^a-z]+$)", "");
				if (w.length() == 0)
					continue;
				set.add(w);
				if (cnt >= 100)
					break;
			}

			String[] words = new String[set.size()];

			words = set.toArray(words);

			for (String w : words) {
				WORD.set(w);
				context.write(WORD, ONE);

			}
		}
	}

	private static class FirstCombiner extends Reducer<Text, FloatWritable, Text, FloatWritable> {
		// Reuse objects.
		private final static FloatWritable SUM = new FloatWritable();

		@Override
		public void reduce(Text key, Iterable<FloatWritable> values, Context context)
				throws IOException, InterruptedException {
			// Sum up values.
			Iterator<FloatWritable> iter = values.iterator();
			int sum = 0;
			while (iter.hasNext()) {
				sum += iter.next().get();
			}
			SUM.set(sum);
			context.write(key, SUM);
		}
	}

	// Reducer: sums up all the occurance of words, filter out those with <10
	// occurances
	private static class FirstReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {
		// Reuse objects.
		private final static FloatWritable SUM = new FloatWritable();

		@Override
		public void reduce(Text key, Iterable<FloatWritable> values, Context context)
				throws IOException, InterruptedException {
			// Sum up values.
			Iterator<FloatWritable> iter = values.iterator();
			int sum = 0;
			while (iter.hasNext()) {
				sum += iter.next().get();
			}
			if (sum >= 10) {
				SUM.set(sum);
				context.write(key, SUM);
			}
		}
	}

	private static class SecondMapper extends Mapper<LongWritable, Text, PairOfStrings, FloatWritable> {
		private static final PairOfStrings PAIR = new PairOfStrings();
		private static final FloatWritable ONE = new FloatWritable(1);


		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = ((Text) value).toString();

			StringTokenizer itr = new StringTokenizer(line);

			int cnt = 0;
			Set<String> set = new HashSet<String>();
			while (itr.hasMoreTokens()) {
				cnt++;
				String w = itr.nextToken().toLowerCase().replaceAll("(^[^a-z]+|[^a-z]+$)", "");
				if (w.length() == 0)
					continue;
				set.add(w);
				if (cnt >= 100)
					break;
			}

			String[] words = new String[set.size()];
			words = set.toArray(words);
			

			for (int i = 0; i < words.length; i++) {
				for (int j = 0; j < words.length; j++) {
					if (i == j)
						continue;
					PAIR.set(words[i],words[j]);
					context.write(PAIR, ONE);
				}
			}
		}
	}

	private static class SecondCombiner extends Reducer<PairOfStrings, FloatWritable, PairOfStrings, FloatWritable> {
		private final static FloatWritable SUM = new FloatWritable();

		@Override
		public void reduce(PairOfStrings key, Iterable<FloatWritable> values, Context context)
				throws IOException, InterruptedException {
			Iterator<FloatWritable> iter = values.iterator();
			int sum = 0;
			while (iter.hasNext()) {
				sum += iter.next().get();
			}

			SUM.set(sum);
			context.write(key, SUM);
		}
	}

	private static class SecondReducer extends Reducer<PairOfStrings, FloatWritable, PairOfStrings, FloatWritable> {
		private final static FloatWritable SUM = new FloatWritable();
    private static Map<String,Float> individualOccurance=new HashMap<String,Float>();
    
		@Override
		public void setup(Context context) throws IOException{
			Configuration conf=context.getConfiguration();
			FileSystem fs=FileSystem.get(conf);
			Path infile=new Path(conf.get("sideDataPath"));
			if (!fs.exists(infile)){
				throw new IOException("File not found in "+infile.toString());
			}
			
			BufferedReader reader = null;
			try {
				FSDataInputStream inf = fs.open(infile);
				InputStreamReader inStream = new InputStreamReader(inf,"UTF-8");
				reader = new BufferedReader(inStream);

			} catch (FileNotFoundException e) {
				throw new IOException("Side data file not found");
			}

			String line;
			while ((line = reader.readLine()) != null) {
				 String[] parts = line.split(" ");
				 if (parts.length!=2){
					 System.out.println("This line has a wrong format: "+line);
				 }
				 else 
					 individualOccurance.put(parts[0], Float.parseFloat(parts[1]));
			 }
			reader.close();	
		}
		
		@Override
		public void reduce(PairOfStrings key, Iterable<FloatWritable> values, Context context)
				throws IOException, InterruptedException {
			Iterator<FloatWritable> iter = values.iterator();
			int sum = 0; //N(x,y)
			while (iter.hasNext()) {
				sum += iter.next().get();
			}

			if (sum >= 10) {
				SUM.set((float)Math.log10(sum*countLine/(individualOccurance.get(key.getLeftElement())*individualOccurance.get(key.getRightElement()))));
				context.write(key,SUM);
			}
		}
	}

	protected static class SecondPartitioner extends Partitioner<PairOfStrings, FloatWritable> {
		@Override
		public int getPartition(PairOfStrings key, FloatWritable value, int numReduceTasks) {
			return (key.getLeftElement().hashCode() & Integer.MAX_VALUE) % numReduceTasks;
		}
	}

	/**
	 * Creates an instance of this tool.
	 */
	public PairsPMI() {
	}

	public static class Args {
		@Option(name = "-input", metaVar = "[path]", required = true, usage = "input path")
		public String input;

		@Option(name = "-output", metaVar = "[path]", required = true, usage = "output path")
		public String output;

		@Option(name = "-reducers", metaVar = "[num]", required = false, usage = "number of reducers")
		public int numReducers = 1;

	}

	/**
	 * Runs this tool.
	 */
	public int run(String[] argv) throws Exception {
		Args args = new Args();
		CmdLineParser parser = new CmdLineParser(args, ParserProperties.defaults().withUsageWidth(100));
		String sideDataPath = "firstMapReduceJob";
		try {
			parser.parseArgument(argv);
		} catch (CmdLineException e) {
			System.err.println(e.getMessage());
			parser.printUsage(System.err);
			return -1;
		}

		LOG.info("Tool: " + PairsPMI.class.getSimpleName() + " first job");
		LOG.info(" - input path: " + args.input);
		LOG.info(" - output path: " + sideDataPath);
		LOG.info(" - number of reducers: " + args.numReducers);

		Configuration conf = getConf();
		conf.set("sideDataPath", sideDataPath);
		Job job1 = Job.getInstance(conf);
		job1.setJobName(PairsPMI.class.getSimpleName());
		job1.setJarByClass(PairsPMI.class);

		job1.setNumReduceTasks(args.numReducers);

		FileInputFormat.setInputPaths(job1, new Path(args.input));
		FileOutputFormat.setOutputPath(job1, new Path(sideDataPath));

		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(FloatWritable.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(FloatWritable.class);
		job1.setOutputFormatClass(TextOutputFormat.class);

		job1.setMapperClass(FirstMapper.class);
		job1.setCombinerClass(FirstCombiner.class);
		job1.setReducerClass(FirstReducer.class);

		// Delete the output directory if it exists already.
		FileSystem.get(conf).delete(new Path(sideDataPath), true);

		long startTime = System.currentTimeMillis();
		job1.waitForCompletion(true);
		LOG.info("First Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");
		LOG.info("Mapreduce count file has lines " + countLine);

		LOG.info("Tool: " + PairsPMI.class.getSimpleName() + " second job");
		LOG.info(" - input path: " + args.input);
		LOG.info(" - output path: " + args.output);
		LOG.info(" - number of reducers: " + args.numReducers);

		conf = getConf();
		Job job2 = Job.getInstance(conf);
		job2.setJobName(PairsPMI.class.getSimpleName());
		job2.setJarByClass(PairsPMI.class);

		job2.setNumReduceTasks(args.numReducers);

		FileInputFormat.setInputPaths(job2, new Path(args.input));
		FileOutputFormat.setOutputPath(job2, new Path(args.output));

		job2.setMapOutputKeyClass(PairOfStrings.class);
		job2.setMapOutputValueClass(FloatWritable.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(FloatWritable.class);
		job2.setOutputFormatClass(TextOutputFormat.class);

		job2.setMapperClass(SecondMapper.class);
		job2.setCombinerClass(SecondCombiner.class);
		job2.setReducerClass(SecondReducer.class);
		job2.setPartitionerClass(SecondPartitioner.class);

		// Delete the output directory if it exists already.
		Path outputDir = new Path(args.output);
		FileSystem.get(conf).delete(outputDir, true);

		startTime = System.currentTimeMillis();
		job2.waitForCompletion(true);
		LOG.info("Second Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");
		LOG.info("Mapreduce count file has lines " + countLine);

		return 0;
	}

	/**
	 * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
	 */
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new PairsPMI(), args);
	}
}
