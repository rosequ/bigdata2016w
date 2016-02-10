package ca.uwaterloo.cs.bigdata2016w.xeniaqian94.assignment4;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import tl.lin.data.array.ArrayListOfFloatsWritable;
import tl.lin.data.array.ArrayListOfIntsWritable;
import tl.lin.data.map.HMapIF;
import tl.lin.data.map.MapIF;

import com.google.common.base.Preconditions;

/**
 * <p>
 * Main driver program for running the basic (non-Schimmy) implementation of
 * PageRank.
 * </p>
 *
 * <p>
 * The starting and ending iterations will correspond to paths
 * <code>/base/path/iterXXXX</code> and <code>/base/path/iterYYYY</code>. As a
 * example, if you specify 0 and 10 as the starting and ending iterations, the
 * driver program will start with the graph structure stored at
 * <code>/base/path/iter0000</code>; final results will be stored at
 * <code>/base/path/iter0010</code>.
 * </p>
 *
 * @see RunPageRankSchimmy
 * @author Jimmy Lin
 * @author Michael Schatz
 */
public class RunPersonalizedPageRankBasic extends Configured implements Tool {
	private static final Logger LOG = Logger.getLogger(RunPersonalizedPageRankBasic.class);

	private static enum PageRank {
		nodes, edges, massMessages, massMessagesSaved, massMessagesReceived, missingStructure
	};

	// Mapper, no in-mapper combining.
	private static class MapClass
			extends Mapper<IntWritable, PageRankNodeMultisource, IntWritable, PageRankNodeMultisource> {

		// The neighbor to which we're sending messages.
		private static final IntWritable neighbor = new IntWritable();

		// Contents of the messages: partial PageRank mass.
		private static final PageRankNodeMultisource intermediateMass = new PageRankNodeMultisource();

		// For passing along node structure.
		private static final PageRankNodeMultisource intermediateStructure = new PageRankNodeMultisource();
		// private static final Integer sourceLength=new Integer(0);

		@Override
		public void map(IntWritable nid, PageRankNodeMultisource node, Context context)
				throws IOException, InterruptedException {
			// Pass along node structure.
			intermediateStructure.setNodeId(node.getNodeId());
			intermediateStructure.setType(PageRankNodeMultisource.Type.Structure);
			intermediateStructure.setAdjacencyList(node.getAdjacenyList());

			context.write(nid, intermediateStructure);

			int massMessages = 0;

			// Distribute PageRank mass to neighbors (along outgoing edges).
			if (node.getAdjacenyList().size() > 0) {
				// Each neighbor gets an equal share of PageRank mass.
				ArrayListOfIntsWritable list = node.getAdjacenyList();

				ArrayListOfFloatsWritable p = node.getPageRankArray();
				ArrayListOfFloatsWritable newP = new ArrayListOfFloatsWritable();
				for (int i = 0; i < p.size(); i++) {
					newP.add(p.get(i) - (float) StrictMath.log(list.size()));

				}
				context.getCounter(PageRank.edges).increment(list.size());

				// Iterate over neighbors.
				for (int i = 0; i < list.size(); i++) {
					neighbor.set(list.get(i));
					intermediateMass.setNodeId(list.get(i));
					intermediateMass.setType(PageRankNodeMultisource.Type.Mass);
					intermediateMass.setPageRankArray(newP);

					// Emit messages with PageRank mass to neighbors.
					context.write(neighbor, intermediateMass);
					massMessages++;
				}
			}

			// Bookkeeping.
			context.getCounter(PageRank.nodes).increment(1);
			context.getCounter(PageRank.massMessages).increment(massMessages);
		}
	}

	// Reduce: sums incoming PageRank contributions, rewrite graph structure.
	private static class ReduceClass
			extends Reducer<IntWritable, PageRankNodeMultisource, IntWritable, PageRankNodeMultisource> {
		// For keeping track of PageRank mass encountered, so we can compute
		// missing PageRank mass lost
		// through dangling nodes.
		private ArrayListOfFloatsWritable totalMass = new ArrayListOfFloatsWritable();
		private static int sourceLength;
		private static final ArrayList sourceList = new ArrayList<Integer>();

		@Override
		public void setup(Context context) throws IOException {
			Configuration conf = context.getConfiguration();
			String[] sourceStringList = context.getConfiguration().getStrings("source", "");

			for (int i = 0; i < sourceStringList.length; i++) {
				sourceList.add(Integer.parseInt(sourceStringList[i]));
				totalMass.add(Float.NEGATIVE_INFINITY);
			}
			sourceLength = sourceStringList.length;

		}

		@Override
		public void reduce(IntWritable nid, Iterable<PageRankNodeMultisource> iterable, Context context)
				throws IOException, InterruptedException {
			Iterator<PageRankNodeMultisource> values = iterable.iterator();

			// Create the node structure that we're going to assemble back
			// together from shuffled pieces.
			PageRankNodeMultisource node = new PageRankNodeMultisource();

			node.setType(PageRankNodeMultisource.Type.Complete);
			node.setNodeId(nid.get());

			int massMessagesReceived = 0;
			int structureReceived = 0;

			ArrayListOfFloatsWritable mass = new ArrayListOfFloatsWritable();
			for (int i = 0; i < sourceLength; i++) {
				mass.add(Float.NEGATIVE_INFINITY);
			}

			while (values.hasNext()) {
				PageRankNodeMultisource n = values.next();

				if (n.getType().equals(PageRankNodeMultisource.Type.Structure)) {
					// This is the structure; update accordingly.
					ArrayListOfIntsWritable list = n.getAdjacenyList();
					structureReceived++;

					node.setAdjacencyList(list);
				} else {
					// This is a message that contains PageRank mass;
					// accumulate.
					for (int i = 0; i < sourceLength; i++) {
						mass.set(i, sumLogProbs(mass.get(i), n.getPageRank(i)));
					}
					massMessagesReceived++;
				}
			}

			// Update the final accumulated PageRank mass.
			node.setPageRankArray(mass);
			context.getCounter(PageRank.massMessagesReceived).increment(massMessagesReceived);

			// Error checking.
			if (structureReceived == 1) {
				// Everything checks out, emit final node structure with updated
				// PageRank value.
				context.write(nid, node);

				// Keep track of total PageRank mass.
				for (int i = 0; i < sourceLength; i++) {
					totalMass.set(i, sumLogProbs(totalMass.get(i), mass.get(i)));

				}
			} else if (structureReceived == 0) {
				// We get into this situation if there exists an edge pointing
				// to a node which has no
				// corresponding node structure (i.e., PageRank mass was passed
				// to a non-existent node)...
				// log and count but move on.
				context.getCounter(PageRank.missingStructure).increment(1);
				LOG.warn("No structure received for nodeid: " + nid.get() + " mass: " + massMessagesReceived);
				// It's important to note that we don't add the PageRank mass to
				// total... if PageRank mass
				// was sent to a non-existent node, it should simply vanish.
			} else {
				// This shouldn't happen!
				throw new RuntimeException("Multiple structure received for nodeid: " + nid.get() + " mass: "
						+ massMessagesReceived + " struct: " + structureReceived);
			}
		}

		@Override
		public void cleanup(Context context) throws IOException {
			Configuration conf = context.getConfiguration();
			String taskId = conf.get("mapred.task.id");
			String path = conf.get("PageRankMassPath");

			Preconditions.checkNotNull(taskId);
			Preconditions.checkNotNull(path);

			// Write to a file the amount of PageRank mass we've seen in this
			// reducer.
			FileSystem fs = FileSystem.get(context.getConfiguration());
			FSDataOutputStream out = fs.create(new Path(path + "/" + taskId), false);
			totalMass.write(out);
			out.close();
		}
	}

	// Mapper that distributes the missing PageRank mass (lost at the dangling
	// nodes) and takes care
	// of the random jump factor.
	private static class MapPageRankMassDistributionClass
			extends Mapper<IntWritable, PageRankNodeMultisource, IntWritable, PageRankNodeMultisource> {
		private ArrayListOfFloatsWritable missingMass = new ArrayListOfFloatsWritable();
		private static int sourceLength;
		private static final ArrayList sourceList = new ArrayList<Integer>();

		@Override
		public void setup(Context context) throws IOException {
			Configuration conf = context.getConfiguration();

			String[] sourceStringList = context.getConfiguration().getStrings("source", "");

			for (int i = 0; i < sourceStringList.length; i++) {
				sourceList.add(Integer.parseInt(sourceStringList[i]));
				String thisConf = "MissingMass" + i;
				missingMass.add(conf.getFloat(thisConf, 0.0f));
			}
			sourceLength = sourceStringList.length;
		}

		@Override
		public void map(IntWritable nid, PageRankNodeMultisource node, Context context)
				throws IOException, InterruptedException {
			ArrayListOfFloatsWritable p = node.getPageRankArray();
			ArrayListOfFloatsWritable newP = new ArrayListOfFloatsWritable();
			float jump;
			float link;
			for (int i = 0; i < sourceLength; i++) {
				if (sourceList.get(i).equals(nid)) {
					LOG.info("This is the " + i + "th source node:" + nid.get());

					link = (float) Math.log(1.0f - ALPHA)
							+ sumLogProbs(p.get(i), (float) (Math.log(missingMass.get(i))));
					jump = (float) (Math.log(ALPHA));

				} else {
					link = (float) Math.log(1.0f - ALPHA) + sumLogProbs(p.get(i), Float.NEGATIVE_INFINITY);
					jump = (float) (Float.NEGATIVE_INFINITY);

				}
				newP.add(sumLogProbs(jump, link));
			}
			node.setPageRankArray(newP);
			context.write(nid, node);
		}
	}

	// Random jump factor.
	private static float ALPHA = 0.15f;
	private static NumberFormat formatter = new DecimalFormat("0000");

	/**
	 * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
	 */
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new RunPersonalizedPageRankBasic(), args);
	}

	public RunPersonalizedPageRankBasic() {
	}

	private static final String BASE = "base";
	private static final String NUM_NODES = "numNodes";
	private static final String START = "start";
	private static final String END = "end";
	private static final String COMBINER = "useCombiner";
	private static final String INMAPPER_COMBINER = "useInMapperCombiner";
	private static final String RANGE = "range";
	private static final String SOURCES = "sources";

	/**
	 * Runs this tool.
	 */
	@SuppressWarnings({ "static-access" })
	public int run(String[] args) throws Exception {
		Options options = new Options();

		options.addOption(new Option(COMBINER, "use combiner"));
		options.addOption(new Option(INMAPPER_COMBINER, "user in-mapper combiner"));
		options.addOption(new Option(RANGE, "use range partitioner"));

		options.addOption(OptionBuilder.withArgName("path").hasArg().withDescription("base path").create(BASE));
		options.addOption(OptionBuilder.withArgName("num").hasArg().withDescription("start iteration").create(START));
		options.addOption(OptionBuilder.withArgName("num").hasArg().withDescription("end iteration").create(END));
		options.addOption(
				OptionBuilder.withArgName("num").hasArg().withDescription("number of nodes").create(NUM_NODES));
		options.addOption(OptionBuilder.withArgName("sources").hasArg().withDescription("sources").create(SOURCES));

		CommandLine cmdline;
		CommandLineParser parser = new GnuParser();

		try {
			cmdline = parser.parse(options, args);
		} catch (ParseException exp) {
			System.err.println("Error parsing command line: " + exp.getMessage());
			return -1;
		}

		if (!cmdline.hasOption(BASE) || !cmdline.hasOption(START) || !cmdline.hasOption(END)
				|| !cmdline.hasOption(NUM_NODES) || !cmdline.hasOption(SOURCES)) {
			System.out.println("args: " + Arrays.toString(args));
			HelpFormatter formatter = new HelpFormatter();
			formatter.setWidth(120);
			formatter.printHelp(this.getClass().getName(), options);
			ToolRunner.printGenericCommandUsage(System.out);
			return -1;
		}

		String basePath = cmdline.getOptionValue(BASE);
		int n = Integer.parseInt(cmdline.getOptionValue(NUM_NODES));
		int s = Integer.parseInt(cmdline.getOptionValue(START));
		int e = Integer.parseInt(cmdline.getOptionValue(END));
		String source = cmdline.getOptionValue(SOURCES);

		boolean useCombiner = cmdline.hasOption(COMBINER);
		boolean useInmapCombiner = cmdline.hasOption(INMAPPER_COMBINER);
		boolean useRange = cmdline.hasOption(RANGE);

		LOG.info("Tool name: RunPageRank");
		LOG.info(" - base path: " + basePath);
		LOG.info(" - num nodes: " + n);
		LOG.info(" - start iteration: " + s);
		LOG.info(" - end iteration: " + e);
		LOG.info(" - source: " + source);
		LOG.info(" - use combiner: " + useCombiner);
		LOG.info(" - use in-mapper combiner: " + useInmapCombiner);
		LOG.info(" - user range partitioner: " + useRange);

		// Iterate PageRank.
		for (int i = s; i < e; i++) {
			iteratePageRank(i, i + 1, basePath, n, useCombiner, useInmapCombiner, source);
		}

		return 0;
	}

	// Run each iteration.
	private void iteratePageRank(int i, int j, String basePath, int numNodes, boolean useCombiner,
			boolean useInMapperCombiner, String source) throws Exception {
		// Each iteration consists of two phases (two MapReduce jobs).

		// Job 1: distribute PageRank mass along outgoing edges.
		ArrayListOfFloatsWritable mass = phase1(i, j, basePath, numNodes, useCombiner, useInMapperCombiner, source);

		// Find out how much PageRank mass got lost at the dangling nodes.
		ArrayListOfFloatsWritable missing = new ArrayListOfFloatsWritable();
		for (int k = 0; k < mass.size(); k++) {
			missing.add(1.0f - (float) StrictMath.exp(mass.get(k)));
		}

		// Job 2: distribute missing mass, take care of random jump factor.
		phase2(i, j, missing, basePath, numNodes, source);
	}

	private ArrayListOfFloatsWritable phase1(int i, int j, String basePath, int numNodes, boolean useCombiner,
			boolean useInMapperCombiner, String source) throws Exception {
		Job job = Job.getInstance(getConf());
		job.setJobName("PageRank:Basic:iteration" + j + ":Phase1");
		job.setJarByClass(RunPersonalizedPageRankBasic.class);

		String in = basePath + "/iter" + formatter.format(i);
		String out = basePath + "/iter" + formatter.format(j) + "t";
		String outm = out + "-mass";

		// We need to actually count the number of part files to get the number
		// of partitions (because
		// the directory might contain _log).
		int numPartitions = 0;
		for (FileStatus s : FileSystem.get(getConf()).listStatus(new Path(in))) {
			if (s.getPath().getName().contains("part-"))
				numPartitions++;
		}

		LOG.info("PageRank: iteration " + j + ": Phase1");
		LOG.info(" - input: " + in);
		LOG.info(" - output: " + out);
		LOG.info(" - nodeCnt: " + numNodes);
		LOG.info(" - useCombiner: " + useCombiner);
		LOG.info(" - useInmapCombiner: " + useInMapperCombiner);
		LOG.info("computed number of partitions: " + numPartitions);

		int numReduceTasks = numPartitions;

		// Delete the output directory if it exists already.
		FileSystem.get(getConf()).delete(new Path(out), true);
		FileSystem.get(getConf()).delete(new Path(outm), true);

		job.getConfiguration().setInt("NodeCount", numNodes);
		job.getConfiguration().setStrings("source", source);
		job.getConfiguration().setBoolean("mapred.map.tasks.speculative.execution", false);
		job.getConfiguration().setBoolean("mapred.reduce.tasks.speculative.execution", false);
		// job.getConfiguration().set("mapred.child.java.opts", "-Xmx2048m");
		job.getConfiguration().set("PageRankMassPath", outm);

		job.setNumReduceTasks(numReduceTasks);

		FileInputFormat.setInputPaths(job, new Path(in));
		FileOutputFormat.setOutputPath(job, new Path(out));

		job.setInputFormatClass(NonSplitableSequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(PageRankNodeMultisource.class);

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(PageRankNodeMultisource.class);

		// job.setMapperClass(useInMapperCombiner ?
		// MapWithInMapperCombiningClass.class : MapClass.class);
		job.setMapperClass(MapClass.class);
		// if (useCombiner) {
		// job.setCombinerClass(CombineClass.class);
		// }

		job.setReducerClass(ReduceClass.class);

		long startTime = System.currentTimeMillis();
		job.waitForCompletion(true);
		System.out.println("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

		ArrayListOfFloatsWritable mass = new ArrayListOfFloatsWritable();
		String[] sourceStringList = job.getConfiguration().getStrings("source", "");
		for (int k = 0; k < sourceStringList.length; k++) {
			mass.add(Float.NEGATIVE_INFINITY);
		}
		FileSystem fs = FileSystem.get(getConf());
		for (FileStatus f : fs.listStatus(new Path(outm))) {
			FSDataInputStream fin = fs.open(f.getPath());
			ArrayListOfFloatsWritable thisMass = new ArrayListOfFloatsWritable();
			thisMass.readFields(fin);
			for (int k = 0; k < sourceStringList.length; k++) {
				mass.set(k, sumLogProbs(mass.get(k), thisMass.get(k)));
			}
			fin.close();
		}

		return mass;
	}

	private void phase2(int i, int j, ArrayListOfFloatsWritable missing, String basePath, int numNodes, String source)
			throws Exception {
		Job job = Job.getInstance(getConf());
		job.setJobName("PageRank:Basic:iteration" + j + ":Phase2");
		job.setJarByClass(RunPersonalizedPageRankBasic.class);

		LOG.info("missing PageRank mass: " + missing);
		LOG.info("number of nodes: " + numNodes);

		String in = basePath + "/iter" + formatter.format(j) + "t";
		String out = basePath + "/iter" + formatter.format(j);

		LOG.info("PageRank: iteration " + j + ": Phase2");
		LOG.info(" - input: " + in);
		LOG.info(" - output: " + out);

		job.getConfiguration().setBoolean("mapred.map.tasks.speculative.execution", false);
		job.getConfiguration().setBoolean("mapred.reduce.tasks.speculative.execution", false);
		for (int k = 0; k < missing.size(); k++) {
			String thisConf = "MissingMass" + k;
			job.getConfiguration().setFloat(thisConf, (float) missing.get(k));
		}
		job.getConfiguration().setInt("NodeCount", numNodes);
		job.getConfiguration().setStrings("source", source);

		job.setNumReduceTasks(0);

		FileInputFormat.setInputPaths(job, new Path(in));
		FileOutputFormat.setOutputPath(job, new Path(out));

		job.setInputFormatClass(NonSplitableSequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(PageRankNodeMultisource.class);

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(PageRankNodeMultisource.class);

		job.setMapperClass(MapPageRankMassDistributionClass.class);

		FileSystem.get(getConf()).delete(new Path(out), true);

		long startTime = System.currentTimeMillis();
		job.waitForCompletion(true);
		System.out.println("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");
	}

	// Adds two log probs.
	private static float sumLogProbs(float a, float b) {
		if (a == Float.NEGATIVE_INFINITY)
			return b;

		if (b == Float.NEGATIVE_INFINITY)
			return a;

		if (a < b) {
			return (float) (b + StrictMath.log1p(StrictMath.exp(a - b)));
		}

		return (float) (a + StrictMath.log1p(StrictMath.exp(b - a)));
	}
}
