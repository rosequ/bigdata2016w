package ca.uwaterloo.cs.bigdata2016w.xeniaqian94.assignment3;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Set;
import java.util.Stack;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ParserProperties;

import tl.lin.data.array.ArrayListWritable;
import tl.lin.data.pair.PairOfInts;
import tl.lin.data.pair.PairOfWritables;

public class BooleanRetrievalCompressed extends Configured implements Tool {
	private MapFile.Reader index;
	private FSDataInputStream collection;
	private Stack<Set<Integer>> stack;
	private String indexPath;
	private int numReduceTasks;
	private FileSystem fs;

	private BooleanRetrievalCompressed() {
	}

	private void initialize(String collectionPath) throws IOException {
		Path pt = new Path(indexPath);
		ContentSummary cs = fs.getContentSummary(pt);
		long fileCount = cs.getFileCount();
		System.out.println("Total file count is \t" + fileCount);
		numReduceTasks = (int) (fileCount - 1); // To hash
		collection = fs.open(new Path(collectionPath));
		stack = new Stack<Set<Integer>>();
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

		for (PairOfInts pair : fetchPostings(term)) {
			set.add(pair.getLeftElement());
		}

		return set;
	}

	private ArrayListWritable<PairOfInts> fetchPostings(String term) throws IOException {
		Text key = new Text();
		BytesWritable value = new BytesWritable();

		key.set(term);
		
		int whichFile=(term.hashCode() & Integer.MAX_VALUE) % numReduceTasks;
		index = new MapFile.Reader(new Path(indexPath + "/part-r-"+String.format("%05d", whichFile)), fs.getConf());
		index.get(key, value);

		byte[] bytes = value.getBytes();

		ByteArrayInputStream oldPostings = new ByteArrayInputStream(bytes);
		DataInputStream inStream = new DataInputStream(oldPostings);

		ArrayListWritable<PairOfInts> postings = new ArrayListWritable<PairOfInts>();

		int docno = 0;
		int gap = 0;
		int tf = -1;
		int df = WritableUtils.readVInt(inStream);

		for (int i = 0; i < df; i++) {
			gap = WritableUtils.readVInt(inStream);
			tf = WritableUtils.readVInt(inStream);
			docno = docno + gap;
			postings.add(new PairOfInts(docno, tf));
		}

		return postings;

	}

	private String fetchLine(long offset) throws IOException {
		collection.seek(offset);
		BufferedReader reader = new BufferedReader(new InputStreamReader(collection));

		return reader.readLine();
	}

	public static class Args {
		@Option(name = "-index", metaVar = "[path]", required = true, usage = "index path")
		public String index;

		@Option(name = "-collection", metaVar = "[path]", required = true, usage = "collection path")
		public String collection;

		@Option(name = "-query", metaVar = "[term]", required = true, usage = "query")
		public String query;
	}

	/**
	 * Runs this tool.
	 */
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

		if (args.collection.endsWith(".gz")) {
			System.out.println("gzipped collection is not seekable: use compressed version!");
			return -1;
		}

		fs = FileSystem.get(new Configuration());

		indexPath=args.index;
		initialize(args.collection);

		System.out.println("Query: " + args.query);
		long startTime = System.currentTimeMillis();
		runQuery(args.query);
		System.out.println("\nquery completed in " + (System.currentTimeMillis() - startTime) + "ms");

		return 1;
	}

	/**
	 * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
	 */
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new BooleanRetrievalCompressed(), args);
	}
}
