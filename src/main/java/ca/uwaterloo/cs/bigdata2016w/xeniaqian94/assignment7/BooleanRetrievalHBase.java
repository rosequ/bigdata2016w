package ca.uwaterloo.cs.bigdata2016w.xeniaqian94.assignment7;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.NavigableSet;
import java.util.Set;
import java.util.Stack;
import java.util.TreeSet;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ParserProperties;

import ca.uwaterloo.cs.bigdata2016w.xeniaqian94.assignment7.HBaseWordCount.Args;
import ca.uwaterloo.cs.bigdata2016w.xeniaqian94.assignment7.HBaseWordCount.MyTableReducer;
import tl.lin.data.array.ArrayListWritable;
import tl.lin.data.pair.PairOfInts;
import tl.lin.data.pair.PairOfWritables;

public class BooleanRetrievalHBase extends Configured implements Tool {
	private static final Logger LOG = Logger.getLogger(BooleanRetrievalHBase.class);
	private Stack<Set<Integer>> stack;
	HTableInterface table;
	private FSDataInputStream collection;
	
	private void initialize(String collectionPath, FileSystem fs) throws IOException {
	   collection = fs.open(new Path(collectionPath));
	    stack = new Stack<Set<Integer>>();
	  }

	public static class Args {
		@Option(name = "-table", metaVar = "[name]", required = true, usage = "HBase table")
		public String table;

		@Option(name = "-config", metaVar = "[path]", required = true, usage = "HBase config")
		public String config;

		@Option(name = "-collection", metaVar = "[path]", required = true, usage = "collection path")
		public String collection;

		@Option(name = "-query", metaVar = "[term]", required = true, usage = "query")
		public String query;
	}

	private void runQuery(String q) throws IOException {
		String[] terms = q.split("\\s+");

		for (String t : terms) {
			if (t.equals("AND")) {
				performAND();
			} else if (t.equals("OR")) {
				performOR();
			} else {
				pushTerm(t);
			}
		}

		Set<Integer> set = stack.pop();

		for (Integer i : set) {
			String line = fetchLine(i);
			System.out.println(i + "\t" + line);
		}
	}

	private void pushTerm(String term) throws IOException {
		stack.push(fetchDocumentSet(term));
	}

	private void performAND() {
		Set<Integer> s1 = stack.pop();
		Set<Integer> s2 = stack.pop();

		Set<Integer> sn = new TreeSet<Integer>();

		for (int n : s1) {
			if (s2.contains(n)) {
				sn.add(n);
			}
		}

		stack.push(sn);
	}

	private void performOR() {
		Set<Integer> s1 = stack.pop();
		Set<Integer> s2 = stack.pop();

		Set<Integer> sn = new TreeSet<Integer>();

		for (int n : s1) {
			sn.add(n);
		}

		for (int n : s2) {
			sn.add(n);
		}

		stack.push(sn);
	}

	private Set<Integer> fetchDocumentSet(String term) throws IOException {
		Set<Integer> set = new TreeSet<Integer>();

		for (Integer docid : fetchPostings(term)) {
			set.add(docid);
		}

		return set;
	}

	private ArrayList<Integer> fetchPostings(String term) throws IOException {
		Text key = new Text();
		PairOfWritables<IntWritable, ArrayListWritable<PairOfInts>> value = new PairOfWritables<IntWritable, ArrayListWritable<PairOfInts>>();

		Get get = new Get(Bytes.toBytes(term));
		Result result = table.get(get);
		NavigableSet<byte[]> docSet = result.getFamilyMap(BuildInvertedIndexHBase.CF).descendingKeySet();
		ArrayList<Integer> docList=new ArrayList<Integer>();
		Iterator<byte[]> iter = docSet.iterator();
		while (iter.hasNext()) {
			docList.add(Bytes.toInt(iter.next()));
		}
		return docList;
	}

	public String fetchLine(long offset) throws IOException {
		collection.seek(offset);
		BufferedReader reader = new BufferedReader(new InputStreamReader(collection));

		String d = reader.readLine();
		return d.length() > 80 ? d.substring(0, 80) + "..." : d;
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

		
		Configuration conf = getConf();
		conf.addResource(new Path(args.config));
		FileSystem fs = FileSystem.get(new Configuration());

	    initialize(args.collection, fs);

		Configuration hbaseConfig = HBaseConfiguration.create(conf);
		HConnection hbaseConnection = HConnectionManager.createConnection(hbaseConfig);
		table = hbaseConnection.getTable(args.table);

		System.out.println("Query: " + args.query);
		long startTime = System.currentTimeMillis();
		runQuery(args.query);
		System.out.println("\nquery completed in " + (System.currentTimeMillis() - startTime) + "ms");

		return 0;
	}

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new BooleanRetrievalHBase(), args);
	}

}
