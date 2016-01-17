package ca.uwaterloo.cs.bigdata2016w.xeniaqian94.assignment1;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
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
import org.apache.hadoop.mapreduce.Reducer;
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

/**
 * Simple word count demo.
 */
public class PairsPMI extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(PairsPMI.class);
  private static int countLine=0;

  
    
  // Mapper: emits (token, 1) for every word occurrence per line
  private static class FirstMapper extends Mapper<LongWritable, Text, Text, FloatWritable> {
    // Reuse objects to save overhead of object creation.
  	private static final FloatWritable ONE = new FloatWritable(1);
    private final static Text WORD = new Text();

    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      String line = ((Text) value).toString();
      countLine+=1;
               
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
			
			for (String w:words){
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

  // Reducer: sums up all the occurance of words, filter out those with <10 occurances
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
      if (sum>=10){
      	SUM.set(sum);
      	context.write(key, SUM);
      }
    }
  }

  /**
   * Creates an instance of this tool.
   */
  public PairsPMI() {}

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

    try {
      parser.parseArgument(argv);
    } catch (CmdLineException e) {
      System.err.println(e.getMessage());
      parser.printUsage(System.err);
      return -1;
    }

    LOG.info("Tool: " + PairsPMI.class.getSimpleName());
    LOG.info(" - input path: " + args.input);
    LOG.info(" - output path: " + args.output);
    LOG.info(" - number of reducers: " + args.numReducers);

    Configuration conf = getConf();
    Job job = Job.getInstance(conf);
    job.setJobName(PairsPMI.class.getSimpleName());
    job.setJarByClass(PairsPMI.class);

    job.setNumReduceTasks(args.numReducers);

    FileInputFormat.setInputPaths(job, new Path(args.input));
    FileOutputFormat.setOutputPath(job, new Path(args.output));

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(FloatWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(FloatWritable.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    job.setMapperClass(FirstMapper.class);
    job.setCombinerClass(FirstCombiner.class);
    job.setReducerClass(FirstReducer.class);

    // Delete the output directory if it exists already.
    Path outputDir = new Path(args.output);
    FileSystem.get(conf).delete(outputDir, true);

    Path infile=new Path("counts.txt");
    FileSystem fs=FileSystem.get(conf);
    FSDataInputStream in=fs.open(infile);
    
    if(!fs.exists(infile)){
      throw new IOException("File Not Found: " + infile.toString());
    }

    BufferedReader reader = null;
    try{
      FSDataInputStream inf = fs.open(infile);
      InputStreamReader inStream = new InputStreamReader(inf);
      reader = new BufferedReader(inStream);

    } catch(FileNotFoundException e){
      throw new IOException("Exception thrown when trying to open file.");
    }


    String line = reader.readLine(); 
    LOG.info("First input line: '" + line + "'");

    reader.close();
    
    long startTime = System.currentTimeMillis();
    job.waitForCompletion(true);
    LOG.info("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");
    LOG.info("Mapreduce count file has lines "+countLine);
    
//    File f=new File(args.input);
//    FileInputStream fis = new FileInputStream(f);
//    BufferedReader br = new BufferedReader(new InputStreamReader(fis));
//    
//  	String line = null;
//  	while ((line = br.readLine()) != null) {
//  		System.out.println(line);
//  	}
//    LOG.info("File read count file has lines "+);
    return 0;
  }

  /**
   * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
   */
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new PairsPMI(), args);
  }
}
