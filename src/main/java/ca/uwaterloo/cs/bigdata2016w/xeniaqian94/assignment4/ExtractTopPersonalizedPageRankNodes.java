package ca.uwaterloo.cs.bigdata2016w.xeniaqian94.assignment4;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import tl.lin.data.pair.PairOfObjectFloat;
import tl.lin.data.queue.TopScoredObjects;

public class ExtractTopPersonalizedPageRankNodes extends Configured implements Tool {
	private static final Logger LOG = Logger.getLogger(ExtractTopPersonalizedPageRankNodes.class);

	public ExtractTopPersonalizedPageRankNodes() {
	}

	private static final String INPUT = "input";
	private static final String OUTPUT = "output";
	private static final String TOP = "top";
	private static final String SOURCES = "sources";

	private static class MapClass extends Mapper<IntWritable, PageRankNode, IntWritable, PageRankNode> {
		IntWritable ONE = new IntWritable(1);

		@Override
		public void map(IntWritable nid, PageRankNode node, Context context) throws IOException, InterruptedException {
			context.write(ONE, node);

		}
	}

	private static class ReduceClass extends Reducer<IntWritable, PageRankNode, IntWritable, FloatWritable> {
		// For keeping track of PageRank mass encountered, so we can compute
		// missing PageRank mass lost
		// through dangling nodes.
		private int source = 0;
		private int top = 0;
		TopScoredObjects<Integer> topN;

		private final static IntWritable ONE = new IntWritable();
		private final static FloatWritable ONEF = new FloatWritable();

		@Override
		public void setup(Context context) throws IOException {
			Configuration conf = context.getConfiguration();

			source = conf.getInt("source", 0);
			top = conf.getInt("top", 0);
			topN = new TopScoredObjects<Integer>(top);
		}

		@Override
		public void reduce(IntWritable nid, Iterable<PageRankNode> iterable, Context context)
				throws IOException, InterruptedException {
			Iterator<PageRankNode> iter = iterable.iterator();

			while (iter.hasNext()) {
				PageRankNode thisNode = iter.next();
				topN.add(thisNode.getNodeId(), thisNode.getPageRank());
			}
			System.out.println("Source: " + source);

			for (PairOfObjectFloat<Integer> pair : topN.extractAll()) {

				int nodeid = ((Integer) pair.getLeftElement());
				float pagerank = (float) Math.exp(pair.getRightElement());
				System.out.println(String.format("%.5f %d", pagerank, nodeid));
				ONE.set(nodeid);
				ONEF.set(pagerank);

				context.write(ONE, ONEF);
			}

		}
	}

	/**
	 * Runs this tool.
	 */
	@SuppressWarnings({ "static-access" })
	public int run(String[] args) throws Exception {
		Options options = new Options();

		options.addOption(OptionBuilder.withArgName("path").hasArg().withDescription("input path").create(INPUT));
		options.addOption(OptionBuilder.withArgName("path").hasArg().withDescription("output path").create(OUTPUT));
		options.addOption(OptionBuilder.withArgName("top").hasArg().withDescription("top").create(TOP));
		options.addOption(OptionBuilder.withArgName("sources").hasArg().withDescription("sources").create(SOURCES));

		CommandLine cmdline;
		CommandLineParser parser = new GnuParser();

		try {
			cmdline = parser.parse(options, args);
		} catch (ParseException exp) {
			System.err.println("Error parsing command line: " + exp.getMessage());
			return -1;
		}

		if (!cmdline.hasOption(INPUT) || !cmdline.hasOption(OUTPUT) || !cmdline.hasOption(TOP)
				|| !cmdline.hasOption(SOURCES)) {
			System.out.println("args: " + Arrays.toString(args));
			HelpFormatter formatter = new HelpFormatter();
			formatter.setWidth(120);
			formatter.printHelp(this.getClass().getName(), options);
			ToolRunner.printGenericCommandUsage(System.out);
			return -1;
		}

		String inputPath = cmdline.getOptionValue(INPUT);
		String outputPath = cmdline.getOptionValue(OUTPUT);
		int top = Integer.parseInt(cmdline.getOptionValue(TOP));
		int source = Integer.parseInt(cmdline.getOptionValue(SOURCES));

		LOG.info("Tool name: " + ExtractTopPersonalizedPageRankNodes.class.getSimpleName());
		LOG.info(" - input: " + inputPath);
		LOG.info(" - output: " + outputPath);
		LOG.info(" - top: " + top);
		LOG.info("Source: " + source);

		Configuration conf = new Configuration();
		conf.setInt("mapred.min.split.size", 1024 * 1024 * 1024);
		conf.setInt("source", source);

		// Delete the output directory if it exists already.
		FileSystem.get(conf).delete(new Path(outputPath), true);

		Job job = Job.getInstance(getConf());
		job.setJobName("ExtractTopPersonalizedPageRankNodes");
		job.setJarByClass(ExtractTopPersonalizedPageRankNodes.class);
		job.getConfiguration().setInt("source", source);
		job.getConfiguration().setInt("top", top);

		job.setNumReduceTasks(1);

		FileInputFormat.setInputPaths(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		job.setInputFormatClass(NonSplitableSequenceFileInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(PageRankNode.class);

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(FloatWritable.class);

		job.setMapperClass(MapClass.class);

		job.setReducerClass(ReduceClass.class);

		long startTime = System.currentTimeMillis();
		job.waitForCompletion(true);
		System.out.println("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

		job.waitForCompletion(true);

		return 0;
	}

	/**
	 * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
	 */
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new ExtractTopPersonalizedPageRankNodes(), args);
	}
}
