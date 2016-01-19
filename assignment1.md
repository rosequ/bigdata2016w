####Question 1.

For the PairsPMI implementation, there are 2 MapReduce jobs chained together. The first MapReduce job does the following: 

1. Its first Mapper takes each line of the input file, tokenize it, get the set of first 100 words and write into context each unique word counted as 1. Each line, with key as "lineNumberCount", is also counted as 1. (Similar to word count but only binary occurance, i.e. x occurs in this line). Input key-value pair is <LongWritable, Text>. Output key-value pair is <Text, FloatWritable>.
2. Its first Combiner is similar to word count. Input key-value pair is <Text, FloatWritable>. Output key-value pair is <Text, FloatWritable>.
3. Its first Reducer is similar to word count. Input key-value pair is <Text, FloatWritable>. Output key-value pair is <Text, FloatWritable>.
4. The first MapReduce job generates side data, stored as (# of reducers) separate files. <code>FileUtil.copyMerge(FileSystem.get(conf), new Path(sideDataPath+"/"), FileSystem.get(conf), new Path(sideDataPath+".txt"), false, getConf(), null);</code> combines all these separate files into one side data files. 
5. Its second Mapper takes each line of the input file, tokenize it, get the set of first 100 words and write into context each unique word pair, counted as 1.(Similar to cooccurance). Input key-value pair is <LongWritable, Text>. Output key-value pair is <PairOfStrings, FloatWritable>.
6. Its second Combiner is similar to that of the bigram. Input key-value pair is <PairOfStrings, FloatWritable>. Output key-value pair is <PairOfStrings, FloatWritable>. Partitioner partitions by the first word in the word pair.
7. Its second Reducer reads in the previous individual word occurance from the side data file, during the setup period. Reduce the word pair counts in a way similar to that of the cooccurance. Ignore all the pairs of words that co-occur in less than 10 lines. Do the simple PMI formula calculation.
<code>SUM.set((float) Math.log10(sum * individualOccurance.get("lineNumberCount")/ (individualOccurance.get(key.getLeftElement()) * individualOccurance.get(key.getRightElement()))));context.write(key, SUM);</code>
Input key-value pair is <PairOfStrings, FloatWritable>. The final output key-value pair is <PairOfStrings, FloatWritable>

For the StripesPMI implementation, there are 2 MapReduce jobs chained together. The first MapReduce job does the following: 

1. The first MapReduce job is the same as the PairsPMI implementation.
2. Its second Mapper takes each line of the input file, tokenize it, get the set of first 100 words and write into context each unique word pair, counted as 1.(Similar to cooccurance). Each line, with key as "lineNumberCount", is also counted as 1. Input key-value pair is <LongWritable, Text>. Output key-value pair is <Text, HMapStFW>.
3. Its second Combiner is similar to that of the bigram. Input key-value pair is <Text, HMapStFW>. Output key-value pair is <Text, HMapStFW>. Partitioner partitions by the first word in the word pair.
4. Its second Reducer reads in the previous individual word occurance from the side data file, during the setup period. Reduce the word pair counts in a way similar to that of the cooccurance. Ignore all the pairs of words that co-occur in less than 10 lines. Do the simple PMI formula calculation.
<code>PAIR.set(key.toString(),term);
SUM.set((float) Math.log10(map.get(term) * individualOccurance.get("lineNumberCount")/(individualOccurance.get(key.toString()) * individualOccurance.get(term)))); </code>
Input key-value pair is <Text, HMapStFW>. One important note is that to validate our StripesPMI, the final output key-value pair is converted back to <PairOfStrings, FloatWritable>






####Question 2.
ran on <code>linux.student.cs.uwaterloo.ca</code>

complete pairs implementation: 55.148 seconds 

complete stripes implementation: 24.051 seconds

####Question 3.
ran on <code>linux.student.cs.uwaterloo.ca</code>

complete pairs implementation: 70.079 seconds

complete stripes implementation: 29.072 seconds

####Question 4.
38599, if (x,y) and (y,x) are the same pairs (symmetric)

77198, if (x,y) and (y,x) are different pairs (asymmetric)

####Question 5.

The pair is "maine" and "anjou". 

NOTE: based on Piazza, Jimmy's answer, we IGNORE lines with zero word in line count https://piazza.com/class/ii64c6llmtx1xf?cid=109

Following is the check script's output, as a reference: 

(maine, anjou)	3.5971177

(anjou, maine)	3.5971177

(milford, haven)	3.5841527

(haven, milford)	3.5841527

(cleopatra's, alexandria)	3.5027547

(alexandria, cleopatra's)	3.5027547

(rosencrantz, guildenstern)	3.5022905

(guildenstern, rosencrantz)	3.5022905

(personae, dramatis)	3.4956598

(dramatis, personae)	3.4956598


#####Why such high PMI:

There are special terms, names, etc. such as "Duke of Anjou and Maine", "Alexandria. CLEOPATRA'S" and "Rosencrantz and Guildenstern" in Shakespeare. Therefore these terms often appears together. P(x,y)≈P(x)≈P(y)

These terms are also not that common pairs such as "a the". N(x,y), N(x), N(y) are all small in absolute count, which makes its N(x,y)/[N(x)*N(y)] value is relatively large. 

####Question 6.

For "tears":

(tears, shed)	2.0757654

(tears, salt)	2.0167875

(tears, eyes)	1.1291423

For "death":

(death, father's)	1.0842273

(death, die)	0.7181347

(death, life)	0.7021099


####Question 7.

For "waterloo":
(waterloo, kitchener)	2.6149967

(waterloo, napoleon)	1.908439

(waterloo, napoleonic)	1.7866182


For "toronto"
(toronto, marlboros)	2.3539958

(toronto, spadina)	2.312603

(toronto, leafs)	2.3108897





