package com.pagerank.distributed;
/*
 * Cloud9: A MapReduce Library for Hadoop
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You may
 * obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0 
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.jobcontrol.Job;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.hadoop.mapred.lib.HashPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import com.pagerank.distributed.io.ArrayListWritableComparable;
import com.pagerank.utility.MapReduceNode;

/**
 * <p>Class to calculate page rank using normalisation after convergence
 * has been reached.</p>
 * <p>Contains four MapReduce Jobs:</br>
 * 		1. Configure the input text file containing the adjacency matrix 
 * 		for documents and their outlinks, to produce a Sequence File of 
 * 		type Text and MapReduceNode object.</br>
 * 		2. Iterative MapReduce Jobs to calculate Page Rank until convergence.</br>
 * 		3. Output Page Ranks in a Text File so it is in a readable form and sorted by 
 * 		descending Page Ranks. This is split into two jobs; one for outputting
 * 		into a readable form and one for sorting.</p> 
 * 
 * @author group24: Kamil Przekwas, Yasaman Sepanj, Manvir Kaur Grewal
 * 
 */
public class MapReduce extends Configured implements Tool {
	private static final Logger sLogger = Logger.getLogger(MapReduce.class);

	private static int numberNodes;
	/**
	 * Random jump factor to be factored into pageRank in the second MapReduce Job
	 */
	private static final Double RANDOM_JUMP_FACTOR = 0.85;
	
	/**
	 * <p><b>Configure Mapper:</b> This takes the adjacency Text File and produces a Sequence File.</p>
	 *
	 * <p>Map Function reads a line from the adjacency text file which is in the following format;</br>
	 * [DOCUMENT URL] [OUTLINK URL] .... [OUTLINK URL]. It will convert this document into a MapReduceNode 
	 * object. Initial Page Rank should be set to 1/Total Number of Outlinks for faster convergence</p>
	 * 
	 */
	public static class ConfigureMapper extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, MapReduceNode> {

		int numberOfNodes;
		
		@Override
		public void configure(JobConf job) {
			
			numberOfNodes = job.getInt("numberOfNodes", 0);
			if(numberOfNodes == 0) {
				System.exit(0);
			}
		}
		/**
		 * Mapping function. 
		 * 
		 * @param key Input key, not used in this example
		 * @param value A line of input Text taken from the data
		 * @param output Map from each link URL (Text) to its corresponding node (MapReduceNode)
		 */
		public void map(LongWritable key, 
				Text value, 
				OutputCollector<Text, MapReduceNode> output,
				Reporter reporter) throws IOException {
			
			//Convert input word into String and tokenize to find words
			String line = ((Text) value).toString();
			if(!line.equals("")) {
				StringTokenizer itr = new StringTokenizer(line, "\t");
				List<Text> outLinkList = new ArrayListWritableComparable<Text>();				Text nodeId = new Text(itr.nextToken());
				while(itr.hasMoreTokens()) {
					outLinkList.add(new Text(itr.nextToken()));
				}
				List<DoubleWritable> pageRank = new ArrayListWritableComparable<DoubleWritable>();
				pageRank.add(new DoubleWritable((double)1/(double)numberOfNodes));
				output.collect(nodeId, new MapReduceNode(pageRank, nodeId, outLinkList));
			}
		}
	}

	/**
	 * <b>Configure Reducer:</b> Identify function
	 *
	 */
	public static class ConfigureReducer extends MapReduceBase implements
			Reducer<Text, MapReduceNode, Text, MapReduceNode> {
		
		/**
		 *  REDUCER FUNCTION
		 *  
		 *  @param key Text link URL
		 *  @param values An iterator over the values associated with this word
		 *  @param output Map from each link URL (Text) to its corresponding MapReduceNode (MapReduceNode)
		 *  @param reporter Used to report progress
		 */
		public void reduce(Text key, Iterator<MapReduceNode> values,
				OutputCollector<Text, MapReduceNode> output, Reporter reporter) throws IOException {
			output.collect(key, values.next());
		}
	}
	
	/**
	 * <p><b>PageRankCalc Mapper:</b> Calculates the contribution given by each node to each of it's outlink</p>
	 *
	 * <p> For non-dangling documents contribution is calculated to be the current page rank divided by the total
	 * number of outlinks. This value is then emitted to all of the document's outlinks.</p>
	 * 
	 * <p>Lastly, you must emit the full node of the current document</p>
	 */
	public static class PageRankCalcMapper extends MapReduceBase implements
			Mapper<Text, MapReduceNode, Text, MapReduceNode> {
		
		/**
		 * MAPPING FUNCTION. 
		 * 
		 * @param key Text which is the document link.
		 * @param value MapReduceNode which represents a document
		 * @param output Map from each link URL (Text) to its corresponding contribution node or the full node for
		 * the current node (MapReduceNode).
		 */
		@Override
		public void map(Text key, MapReduceNode value,
				OutputCollector<Text, MapReduceNode> output, Reporter reporter)
				throws IOException {
			
			if( value.getNumbrOfOutlinks() > 0 ) {
				value.setContribution(new DoubleWritable(value.getCurrentPageRank().get()/value.getNumbrOfOutlinks()));
				for(Text outLinkNodeID : value.getOutlinkNodeIDs()) {
					output.collect(outLinkNodeID, new MapReduceNode(value.getNodeID(), value.getContribution(), MapReduceNode.TYPE_CONTRIBUTION_NODE));
				}
			}
			output.collect(key, value);
		}
		
	}
	
	/**
	 * <p><b>PageRankCalc Reducer:</b> Sums up the contribution for this document from all of it's inlinks 
	 * and stores the full MapReduceNode for this document.</p>
	 *
	 * <p>Before emitting the Map Reduce Node update it's current page rank foctoring 
	 * in the random jump factor.</p>
	 */
	public static class PageRankCalcReducer extends MapReduceBase implements 
					Reducer<Text, MapReduceNode, Text, MapReduceNode> {

		/**
		 *  REDUCER FUNCTION
		 *  
		 *  @param key, Text link URL
		 *  @param values, An iterator over the Map Reduce Nodes associated with this link
		 *  @param output Map from each link URL (Text) to its corresponding MapReduceNode. (MapReduceNode)
		 *  @param reporter Used to report progress
		 */
		
		int numberOfNodes;
		
		@Override
		public void configure(JobConf job) {
			
			numberOfNodes = job.getInt("numberOfNodes", 0);
			if(numberOfNodes == 0) {
				System.exit(0);
			}
		}
		
		@Override
		public void reduce(Text key, Iterator<MapReduceNode> values,
				OutputCollector<Text, MapReduceNode> output, Reporter reporter)
				throws IOException {
			
			
			Double pageRankContributionSum = 0.0;
			MapReduceNode keyNode = null;
			while(values.hasNext()) {
				MapReduceNode current = values.next();
				int currentType = current.getType();
				
				if(currentType == MapReduceNode.TYPE_FULL_NODE) {
					keyNode = current.copyNode();
				} else{
					pageRankContributionSum += current.getContribution().get();
				}
			}

			keyNode.addPagerank(new DoubleWritable((double)(1-RANDOM_JUMP_FACTOR)/(double)numberOfNodes
					+ (RANDOM_JUMP_FACTOR*(pageRankContributionSum))));

			output.collect(key, keyNode);
		}
	}
	
	/**
	 * <p><b>Normalisation Mapper:</b>Emits the each document and its final page rank value and
	 * also a special key with the current documents final page rank to allow for calculation
	 * of the total page rank for all documents in the reducer.</p>
	 *
	 */
	public static class NormalisationMapper extends MapReduceBase implements
	Mapper<Text, MapReduceNode, Text, DoubleWritable> {

		private static final Text special_key = new Text("*");
		
		/**
		 * MAPPING FUNCTION. 
		 * 
		 * @param key Text which is the document link.
		 * @param value MapReduceNode which represents a document
		 * @param output Map from each link URL or a special key (Text) and final page rank. (DoubleWritable)
		 * 
		 */
		@Override
		public void map(Text key, MapReduceNode value,
				OutputCollector<Text, DoubleWritable> output, Reporter reporter)
						throws IOException {
			
			DoubleWritable pageRank = value.getCurrentPageRank();
			output.collect(key, pageRank);
			output.collect(special_key, pageRank);
		}
	}

	/**
	 * <p><b>Normalisation Reducer:</b>Normalises the final page rank values for each document.</p>
	 * 
	 * <p>The special key emitted from the mapper should appear first in the reducer which allows summation
	 * of the total page rank of all documents.</p>
	 * 
	 * <p>Before emitting the document link and final page rank it must be normalised by dividing the 
	 * current page rank by the total page rank summation for all documents</p>
	 *
	 */
	public static class NormalisationReducer extends MapReduceBase implements 
	Reducer<Text, DoubleWritable, Text, DoubleWritable> {

		private double normalisationFactor = 0.0;
		DoubleWritable finalPagerank = new DoubleWritable();
		
		/**
		 *  REDUCER FUNCTION
		 *  
		 *  @param key, Text link URL
		 *  @param values, An iterator over DoubleWritable PageRank for this document. This should only have one value.
		 *  @param output Map from each link URL (Text) to its corresponding Normalised Page Rank value. (DoubleWritable)
		 *  @param reporter Used to report progress
		 */
		@Override
		public void reduce(Text key, Iterator<DoubleWritable> values,
				OutputCollector<Text, DoubleWritable> output, Reporter reporter)
						throws IOException {

			if(key.toString().equals("*")){
				while(values.hasNext()){
					normalisationFactor += values.next().get();
				}
			}
			else{
				sLogger.info(" Normalization: normalisationFactor: " + normalisationFactor);
				finalPagerank.set(((values.next().get())/normalisationFactor));
				output.collect(key,finalPagerank);
			}
		}
	}
	
	/**
	 * <p><b>Output Mapper:</b> Switches the key and value for each document 
	 * to allow sorting by PageRank value when sent to the reducer.</p>
	 *
	 */
	public static class OutputMapper extends MapReduceBase implements
	Mapper<Text, DoubleWritable, DoubleWritable, Text> {

		
		/**
		 * MAPPING FUNCTION. 
		 * 
		 * @param key Text which is the document link.
		 * @param value DoubleWritable which represents a documents Normalised Page Rank.
		 * @param output Map from a DoubleWritable to its document link.  
		 */
		@Override
		public void map(Text key, DoubleWritable value,
				OutputCollector<DoubleWritable, Text> output, Reporter reporter)
						throws IOException {
			
			output.collect(value, key);
		}
	}
	
	/**
	 * <p><b>Output Reducer:</b> Identity Function</p>
	 *
	 * <p>Before emitting the Map Reduce Node update it's current page rank.</p>
	 */
	public static class OutputReducer extends MapReduceBase implements 
	Reducer<DoubleWritable, Text, DoubleWritable, Text> {

		/**
		 *  REDUCER FUNCTION
		 *  
		 *  @param key, DoubleWritable PageRank value.
		 *  @param values, An iterator over the Text links associated with this link. This could be more than one if documents have the same
		 *  PageRank values so you must check for this.
		 *  @param output Map from each PageRank value (DoubleWritable) to its corresponding document URL. (Text)
		 *  @param reporter Used to report progress
		 */
		@Override
		public void reduce(DoubleWritable key, Iterator<Text> values,
				OutputCollector<DoubleWritable, Text> output, Reporter reporter)
						throws IOException {
			
			while(values.hasNext()){
				output.collect(key, values.next());
			}

		}
	}
	
	/**
	 * <p><b>Reverse Comparator:</b> Ensures that the Page Rank values are sorted in descending order
	 * rather than in the default ascending order.</p>
	 *
	 */
	static class ReverseComparator extends WritableComparator {
		
        private static final DoubleWritable.Comparator DOUBLE_COMPARATOR = new DoubleWritable.Comparator();

        public ReverseComparator() {
            super(DoubleWritable.class);
        }

        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return (-1)* DOUBLE_COMPARATOR
			        .compare(b1, s1, l1, b2, s2, l2);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            if (a instanceof DoubleWritable && b instanceof DoubleWritable) {
                return (-1)*(((DoubleWritable) a)
                        .compareTo((DoubleWritable) b));
            }
            return super.compare(a, b);
        }
    }
	
	/**
	 * Creates an instance of this tool.
	 */
	public MapReduce() {}

	/**
	 *  Prints argument options
	 * @return
	 */
	protected static int printUsage() {
		System.out.println("usage: [input-path] [output-path] [num-mappers] [num-reducers]");
		ToolRunner.printGenericCommandUsage(System.out);
		return -1;
	}

	/**
	 * Runs this tool.
	 */
	public int run(String[] args) throws Exception {
		
		long startWholeProgram = System.currentTimeMillis();
		boolean hasConverged = false;
		int counter = 0;
		Reader sequenceFilereader;
		
		//Must have four arguments
		if (args.length != 4) {
			MapReduce.printUsage();
			return -1;
		}
		
		//Set input and output file paths
		String inputPathToAdjacencyTextFile = args[0];
		String outputPathToNodeSequenceFileFormat = args[1];
		
		//Configure Job 1: 
		int mapTasks = Integer.parseInt(args[2]);
		int reduceTasks = Integer.parseInt(args[3]);
		int reduceTasksSetup = 1;
		
		//Configure Job Setup
		JobConf conf1 = new JobConf(MapReduce.class);
		conf1.setInt("numberOfNodes", numberNodes);
		conf1.setJobName("Setup Job");
		conf1.setNumMapTasks(mapTasks);
		conf1.setNumReduceTasks(reduceTasksSetup);
		
		FileInputFormat.setInputPaths(conf1, new Path(inputPathToAdjacencyTextFile));
		FileOutputFormat.setOutputPath(conf1, new Path(outputPathToNodeSequenceFileFormat));
		FileOutputFormat.setCompressOutput(conf1, false);
		
		conf1.setOutputKeyClass(Text.class);
		conf1.setOutputValueClass(MapReduceNode.class);
		
		conf1.setOutputFormat(SequenceFileOutputFormat.class);
		conf1.setInputFormat(TextInputFormat.class);
		
		conf1.setMapperClass(ConfigureMapper.class);
		conf1.setReducerClass(ConfigureReducer.class);
		
		//Delete the output directory if it exists already
		Path tempDir = new Path(outputPathToNodeSequenceFileFormat);
		FileSystem.get(tempDir.toUri(), conf1).delete(tempDir, true);
		
		long startTime = System.currentTimeMillis();
		
		//Run Configure Job
		RunningJob job = JobClient.runJob(conf1);
		
		sLogger.info("Config Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0
				+ " seconds");

		String inputPath = args[1];
		String outputPath = null;
		String outputPathForNormalisedPagerank = null;
		String outputPathforFinalSortedPagerank = null;
		
		//Page Rank Calculation Job 2 (Iterative until convergence has been reached)

		while(!hasConverged) {
			System.out.println("*** ITERATION " + counter + ", number of nodes: " + numberNodes);
			counter++;
			
			sLogger.info("***** ITERATION " + counter);
			
			outputPath = args[1] + counter;
			
			//Pure Page Rank Calculation Job Setup
			JobConf pageRankJob = new JobConf(getConf(), MapReduce.class);
			pageRankJob.setInt("numberOfNodes", numberNodes);
			
			FileInputFormat.setInputPaths(pageRankJob, new Path(inputPath));
			FileOutputFormat.setOutputPath(pageRankJob, new Path(outputPath));
			FileOutputFormat.setCompressOutput(pageRankJob, false);
			
			pageRankJob.setJobName("PP Iteration " + counter);
			pageRankJob.setNumMapTasks(mapTasks);
			pageRankJob.setNumReduceTasks(reduceTasks);
			
			pageRankJob.setOutputKeyClass(Text.class);
			pageRankJob.setOutputValueClass(MapReduceNode.class);
			
			pageRankJob.setOutputFormat(SequenceFileOutputFormat.class);
			pageRankJob.setInputFormat(SequenceFileInputFormat.class);
			
			pageRankJob.setMapperClass(MapReduce.PageRankCalcMapper.class);
			pageRankJob.setReducerClass(MapReduce.PageRankCalcReducer.class);
			
			//Delete the output directory if it exists already
			Path tempPageRankDir = new Path(outputPath);
			FileSystem.get(tempDir.toUri(), conf1).delete(tempPageRankDir, true);
			
			startTime = System.currentTimeMillis();
			
			//Run Pure Page Rank Calculation Job
			RunningJob runningJob = JobClient.runJob(pageRankJob);
			
			sLogger.info("PP Job"+ counter + "Finished in " + (System.currentTimeMillis() - startTime) / 1000.0
					+ " seconds");
			
			//Delete the input directory if it exists already
			Path tempInputPageRankDir = new Path(inputPath);
			FileSystem.get(tempDir.toUri(), conf1).delete(tempInputPageRankDir, true);
			
			//Set the output path of this iteration to be the inputpath for the next iteration
			inputPath = outputPath;
			
			//Check for convergence after every five iterations
			if(counter % 5 == 0){
				
				Configuration conf = getConf();
				if(outputPath != null) {
					sLogger.info("Attempting to open file: " + outputPath + File.separator + "part-00000");
					System.out.println("Attempting to open file: " + outputPath + File.separator + "part-00000");
				} else {
					sLogger.info("OUTPUT PATH IS NULL");
					System.out.println("OUTPUT PATH IS NULL");
				}
				Path cFile = new Path(outputPath + File.separator + "part-00000");
				FileSystem fs = FileSystem.get(cFile.toUri(), conf);	
			
				sequenceFilereader = new Reader(fs, cFile, conf);

				for(int i=0; i<5 ; i++){
					MapReduceNode readValue = new MapReduceNode();
					Text readKey = new Text();
					
					sequenceFilereader.next(readKey, readValue);
					if(!(readValue.hasConverged())){
						break;
					}
					
					if(i==4){
						hasConverged = true;
						sequenceFilereader.close();
					}
				}
				sequenceFilereader.close();
			}
			if(counter == 75) {
				sLogger.info("****************** Exiting (purposefully) after 75th iteration");
				hasConverged = true;
			}
		}
		
		//Normalised Page Rank Calculation Job 3 
		outputPathForNormalisedPagerank = args[1]+"normalizedPageRank";
		
		//Normalised Page Rank Calculation Job Setup
		JobConf normalizationJob = new JobConf(getConf(), MapReduce.class);
		
		FileInputFormat.setInputPaths(normalizationJob, new Path(inputPath));
		FileOutputFormat.setOutputPath(normalizationJob, new Path(outputPathForNormalisedPagerank));
		FileOutputFormat.setCompressOutput(normalizationJob, false);
		
		normalizationJob.setJobName("Normalised Pagerank Output");
		normalizationJob.setNumMapTasks(mapTasks);
		normalizationJob.setNumReduceTasks(1);
		
		normalizationJob.setOutputKeyClass(Text.class);
		normalizationJob.setOutputValueClass(DoubleWritable.class);

		normalizationJob.setInputFormat(SequenceFileInputFormat.class);
		normalizationJob.setOutputFormat(SequenceFileOutputFormat.class);
		
		normalizationJob.setMapperClass(NormalisationMapper.class);
		normalizationJob.setReducerClass(NormalisationReducer.class);
		
		//Delete the output directory if it exists already
		Path tempUpdatedPageRankDir = new Path(outputPathForNormalisedPagerank);
		FileSystem.get(tempDir.toUri(), conf1).delete(tempUpdatedPageRankDir, true);
		
		startTime = System.currentTimeMillis();
		
		//Run Normalised Page Rank Calculation Job
		RunningJob runningUpdateJob = JobClient.runJob(normalizationJob);
		
		sLogger.info("Normalisation Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0
				+ " seconds");
		
		//Sorting and Output Job 4
		
		//Delete the intermediary files created
		Path tempNormalizationInputPath = new Path(inputPath);
		FileSystem.get(tempDir.toUri(), conf1).delete(tempNormalizationInputPath, true);
		
		inputPath = outputPathForNormalisedPagerank;
		outputPathforFinalSortedPagerank = args[1]+"FinalSortedPageRank";
		
		//Sorting and Output Job Setup
		JobConf outputJob = new JobConf(getConf(), MapReduce.class);

		FileInputFormat.setInputPaths(outputJob, new Path(inputPath));
		FileOutputFormat.setOutputPath(outputJob, new Path(outputPathforFinalSortedPagerank));
		FileOutputFormat.setCompressOutput(outputJob, false);

		outputJob.setJobName("Final Pagerank Output");
		sLogger.info("Starting final sotirng job -> this will output a single file");
		outputJob.setNumMapTasks(1);
		outputJob.setNumReduceTasks(1);

		outputJob.setOutputKeyClass(DoubleWritable.class);
		outputJob.setOutputValueClass(Text.class);

		outputJob.setInputFormat(SequenceFileInputFormat.class);
		outputJob.setOutputFormat(TextOutputFormat.class);

		outputJob.setMapperClass(OutputMapper.class);
		outputJob.setReducerClass(OutputReducer.class);
		
		outputJob.setOutputKeyComparatorClass(ReverseComparator.class);

		startTime = System.currentTimeMillis();

		//Run Sorting and Output Job
		RunningJob runningSortingJob = JobClient.runJob(outputJob);
		
		//Delete the intermediary files created
		Path tempFinalSortedInputPath = new Path(inputPath);
		FileSystem.get(tempDir.toUri(), conf1).delete(tempFinalSortedInputPath, true);

		sLogger.info("Final Sorting Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0
				+ " seconds");


		sLogger.info("The program lasted " + (System.currentTimeMillis() - startWholeProgram) / 1000.0 + 
				"s (" + (System.currentTimeMillis() - startWholeProgram) / 60000.0 + " mins)");
		
		return 0;
	}

	/**
	 * Dispatches command-line arguments to the tool via the
	 * <code>ToolRunner</code>.
	 */
	public static void main(String[] args) throws Exception {

		//Count the total number of documents
		String collectionPath = args[0];
		Configuration config = new Configuration();
		Path path = new Path(collectionPath);
	    FileSystem fs = FileSystem.get(path.toUri(), config);

	    FSDataInputStream collection = fs.open(path);
	    BufferedReader reader = new BufferedReader(new InputStreamReader(collection));
        
		int count = 0;
		while(reader.readLine() != null) {
			count++;
		}
		
		numberNodes = count;
		
		int res = ToolRunner.run(new Configuration(), new MapReduce(), args);
		System.exit(res);
	}

}
