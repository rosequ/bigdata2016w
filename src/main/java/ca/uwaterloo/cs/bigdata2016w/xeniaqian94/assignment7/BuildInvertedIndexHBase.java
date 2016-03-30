package ca.uwaterloo.cs.bigdata2016w.xeniaqian94.assignment7;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ParserProperties;

import ca.uwaterloo.cs.bigdata2016w.xeniaqian94.assignment7.BuildInvertedIndex.MyMapper;
import ca.uwaterloo.cs.bigdata2016w.xeniaqian94.assignment7.BuildInvertedIndex.MyReducer;
import tl.lin.data.array.ArrayListWritable;
import tl.lin.data.pair.PairOfInts;
import tl.lin.data.pair.PairOfWritables;

public class BuildInvertedIndexHBase extends Configured implements Tool {
	private static final Logger LOG = Logger.getLogger(BuildInvertedIndexHBase.class);

	public static final String[] FAMILIES = { "p" };
	public static final byte[] CF = FAMILIES[0].getBytes();
	public static final byte[] COUNT = "count".getBytes();

	public static class MyTableReducer extends TableReducer<Text, PairOfInts, ImmutableBytesWritable> {
		public void reduce(Text key, Iterable<PairOfInts> values, Context context)
				throws IOException, InterruptedException {
			Put put = new Put(Bytes.toBytes(key.toString()));
			Iterator<PairOfInts> iter = values.iterator();
			ArrayListWritable<PairOfInts> postings = new ArrayListWritable<PairOfInts>();
			while (iter.hasNext()) {
				postings.add(iter.next().clone());
			}
			Collections.sort(postings);
			iter = postings.iterator();
			while (iter.hasNext()) {
				int docno = iter.next().getLeftElement();
				int termFreq = iter.next().getRightElement();
				put.add(CF, Bytes.toBytes(docno), Bytes.toBytes(termFreq));
			}
			context.write(null, put);
		}
	}

	public static class Args {
		@Option(name = "-input", metaVar = "[path]", required = true, usage = "input path")
		public String input;

		@Option(name = "-table", metaVar = "[name]", required = true, usage = "HBase table to store output")
		public String table;

		@Option(name = "-config", metaVar = "[path]", required = true, usage = "HBase config")
		public String config;

		@Option(name = "-reducers", metaVar = "[num]", required = false, usage = "number of reducers")
		public int numReducers = 1;
	}

	@Override
	public int run(String[] argv) throws Exception {
		Args args = new Args();
		CmdLineParser parser = new CmdLineParser(args, ParserProperties.defaults().withUsageWidth(100));

		try {
			parser.parseArgument(argv);
		} catch (CmdLineException e) {
			System.err.println(e.getMessage());
			parser.printUsage(System.err);
			return -1;
		}

		LOG.info("Tool: " + BuildInvertedIndexHBase.class.getSimpleName());
		LOG.info(" - input path: " + args.input);
		LOG.info(" - output table: " + args.table);
		LOG.info(" - config: " + args.config);
		LOG.info(" - number of reducers: " + args.numReducers);

		// If the table doesn't already exist, create it.
		Configuration conf = getConf();
		conf.addResource(new Path(args.config));

		Configuration hbaseConfig = HBaseConfiguration.create(conf);
		HBaseAdmin admin = new HBaseAdmin(hbaseConfig);

		if (admin.tableExists(args.table)) {
			LOG.info(String.format("Table '%s' exists: dropping table and recreating.", args.table));
			LOG.info(String.format("Disabling table '%s'", args.table));
			admin.disableTable(args.table);
			LOG.info(String.format("Droppping table '%s'", args.table));
			admin.deleteTable(args.table);
		}

		HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf(args.table));
		for (int i = 0; i < FAMILIES.length; i++) {
			HColumnDescriptor hColumnDesc = new HColumnDescriptor(FAMILIES[i]);
			tableDesc.addFamily(hColumnDesc);
		}
		admin.createTable(tableDesc);
		LOG.info(String.format("Successfully created table '%s'", args.table));

		admin.close();

		// Now we're ready to start running MapReduce.
		Job job = Job.getInstance(conf);
		job.setJobName(BuildInvertedIndexHBase.class.getSimpleName());
		job.setJarByClass(BuildInvertedIndexHBase.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(PairOfInts.class);
		job.setMapperClass(BuildInvertedIndex.MyMapper.class);
		// job.setCombinerClass(BuildInvertedIndex.MyReducer.class);
		job.setNumReduceTasks(args.numReducers);

		FileInputFormat.setInputPaths(job, new Path(args.input));
		TableMapReduceUtil.initTableReducerJob(args.table, MyTableReducer.class, job);

		long startTime = System.currentTimeMillis();
		job.waitForCompletion(true);
		LOG.info("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

		return 0;
	}

	/**
	 * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
	 */
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new BuildInvertedIndexHBase(), args);
	}

}
