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
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;

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
import org.apache.hadoop.io.Text;
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
 * <p>Class to calculate page rank using normalisation at each iteration
 * by redistributing missing mass collected throughout from dangling nodes 
 * equally to all nodes in the graph.</p>
 * <p>Contains four MapReduce Jobs:</br>
 * 		1. Configure the input text file containing the adjacency matrix 
 * 		for documents and their outlinks, to produce a Sequence File of 
 * 		type Text and MapReduceNode object.</br>
 * 		2. Iterative MapReduce Jobs to calculate Page Rank until convergence. This
 * 		contains two jobs, one for calculating pure page rank and another to
 * 		update page rank factoring in missing mass and the random jump factor.</br>
 * 		3. Output Page Ranks in a Text File so it is in a readable form.</p> 
 * 
 * @deprecated This class has been replaced by MapReduce.java class.
 */
public class MapReducePageRankWithMissingMass extends Configured implements Tool {
	private static final Logger sLogger = Logger.getLogger(MapReducePageRankWithMissingMass.class);

	private static int numberNodes;
	private static int numberOfReducers;;
	private static final Text MISSING_MASS_SPECIAL_KEY = new Text("MISSING MASS");
	
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

		/**
		 * MAPPING FUNCTION. 
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
				List<Text> outLinkList = new ArrayListWritableComparable<Text>();
				Text nodeId = new Text(itr.nextToken());
				while(itr.hasMoreTokens()) {
					outLinkList.add(new Text(itr.nextToken()));
				}
				List<DoubleWritable> pageRank = new ArrayListWritableComparable<DoubleWritable>();
				pageRank.add(new DoubleWritable((double)1/numberNodes));
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
	 * <p>For dangling nodes, the page rank is emitted under a special key for collecting missing mass.</p>
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
		 * the current node (MapReduceNode) or special missing mass node.
		 */
		public void map(Text key, MapReduceNode value,
				OutputCollector<Text, MapReduceNode> output, Reporter reporter)
				throws IOException {
			
			if( value.getNumbrOfOutlinks() > 0 ) {
				value.setContribution(new DoubleWritable(value.getCurrentPageRank().get()/value.getNumbrOfOutlinks()));
				for(Text outLinkNodeID : value.getOutlinkNodeIDs()) {
					output.collect(outLinkNodeID, new MapReduceNode(value.getNodeID(), value.getContribution(), MapReduceNode.TYPE_CONTRIBUTION_NODE));
				}
			} else {
				output.collect(MISSING_MASS_SPECIAL_KEY, new MapReduceNode(MISSING_MASS_SPECIAL_KEY, value.getCurrentPageRank(), MapReduceNode.TYPE_MASS_NODE));
			}
			output.collect(key, value);
		}
		
	}
	
	/**
	 * <p><b>PageRankCalc Reducer:</b> Sums up the contribution for this document from all of it's inlinks or
	 * the total missing mass and stores the full MapReduceNode for this document.</p>
	 *
	 * <p>Before emitting the Map Reduce Node update it's current page rank.</p>
	 */
	public static class PageRankCalcReducer extends MapReduceBase implements 
					Reducer<Text, MapReduceNode, Text, MapReduceNode> {

		/**
		 *  REDUCER FUNCTION
		 *  
		 *  @param key, Text link URL
		 *  @param values, An iterator over the Map Reduce Nodes associated with this link
		 *  @param output Map from each link URL (Text) to its corresponding MapReduceNode or special missing mass node. (MapReduceNode)
		 *  @param reporter Used to report progress
		 */
		@Override
		public void reduce(Text key, Iterator<MapReduceNode> values,
				OutputCollector<Text, MapReduceNode> output, Reporter reporter)
				throws IOException {
			
			
			Double pageRankContributionSum = 0.0;
			MapReduceNode keyNode = null;
			while(values.hasNext()) {
				MapReduceNode current = values.next();
				int currentType = current.getType();
				if(currentType == MapReduceNode.TYPE_CONTRIBUTION_NODE || currentType == MapReduceNode.TYPE_MASS_NODE) {
					pageRankContributionSum += current.getContribution().get();
				} else if(currentType == MapReduceNode.TYPE_FULL_NODE) {
					keyNode = current.copyNode();
				}
			}
			if(key.equals(MISSING_MASS_SPECIAL_KEY)) {

				keyNode = new MapReduceNode(MISSING_MASS_SPECIAL_KEY, 
						new DoubleWritable(pageRankContributionSum), 
								MapReduceNode.TYPE_MASS_NODE);
				output.collect(key, keyNode);
			} else  {
				keyNode.addPagerank(new DoubleWritable(pageRankContributionSum));
				output.collect(key, keyNode);
			}
		}
		
	}
	
	/**
	 * <p><b>UpdatePageRank Mapper:</b> Calculates the amount of missing mass being redistributed
	 * to each document.</p>
	 *
	 */
	public static class UpdatePageRankMap extends MapReduceBase implements
					Mapper<Text, MapReduceNode, Text, MapReduceNode> {
		
		/**
		 * MAPPING FUNCTION. 
		 * 
		 * @param key Text which is the document link.
		 * @param value MapReduceNode which represents a document
		 * @param output Map from each link URL (Text) to its full map reduce node or special 
		 * missing mass node with the mass being distributed to each node.
		 */
		@Override
		public void map(Text key, MapReduceNode value,
				OutputCollector<Text, MapReduceNode> output, Reporter reporter)
				throws IOException {
			
			if(key.equals(MISSING_MASS_SPECIAL_KEY)){
				
				Double missingMassContribution = value.getContribution().get()/(double)numberNodes;
				
				//Need to emit missing mass node for each reducer and ensure it appears first.
				for(int i=0 ; i<numberOfReducers ; i++ ){
					value.setContribution(new DoubleWritable(missingMassContribution));
					output.collect(new Text("*" + "_" + i), value);
				}
				
			} else {
				output.collect(key, value);
			}
			
		}
		
	}
	
	/**
	 * <p><b>PageRank Partitioner:</b> Ensures that the missing mass distribution value
	 * to each of the reducers to update page rank of each document.</p>
	 *
	 */
	public static final class PageRankPartitioner implements Partitioner {

		@Override
		public void configure(JobConf job) {
			
		}

		@Override
		public int getPartition(Object key, Object value, int numPartitions) {
			
			if(((Text) key).toString().startsWith("*")){
				
				int reducerNumber = Integer.parseInt((((Text) key).toString().split("\\_"))[1]);
				
				return (reducerNumber+1) % numPartitions;
				
			} else {
				
				return (key.hashCode() & Integer.MAX_VALUE) % numPartitions;
			}
		}
		
	}
	
	/**
	 * <p><b>UpdatePageRank Reducer:</b> Updates the current page rank of every document
	 * to factor in the random jump factor and the missing mass contribution</p>
	 *
	 * <p>Missing Mass Contribution should appear first in the reducer.</p>
	 */
	public static class UpdatePageRankReducer extends MapReduceBase implements 
	Reducer<Text, MapReduceNode, Text, MapReduceNode> {

		double missingMassContribution = 0.0;
		
		/**
		 *  REDUCER FUNCTION
		 *  
		 *  @param key, Text link URL
		 *  @param values, An iterator over the Map Reduce Nodes associated with this link
		 *  @param output Map from each link URL (Text) to its corresponding MapReduceNode (MapReduceNode)
		 *  @param reporter Used to report progress
		 */
		@Override
		public void reduce(Text key, Iterator<MapReduceNode> values,
				OutputCollector<Text, MapReduceNode> output, Reporter reporter)
						throws IOException {
			
			MapReduceNode value = values.next();
			if(key.toString().startsWith("*")) {
				missingMassContribution = value.getContribution().get();
				
			} else {
				value.updateCurrentPageRank(new DoubleWritable((1-RANDOM_JUMP_FACTOR)/numberNodes
						+ (RANDOM_JUMP_FACTOR *(value.getCurrentPageRank().get() + missingMassContribution))));
				
				output.collect(key, value);
			}
			
		}

	}
	
	/**
	 * <p><b>Output Mapper:</b> Output the document link and the page rank to a text file</p>
	 *
	 */
	public static class OutputMap extends MapReduceBase implements
	Mapper<Text, MapReduceNode, Text, DoubleWritable> {

		/**
		 * MAPPING FUNCTION. 
		 * 
		 * @param key Text which is the document link.
		 * @param value MapReduceNode which represents a document
		 * @param output Map from each link URL (Text) to its final page rank value (DoubleWritable)
		 */
		@Override
		public void map(Text key, MapReduceNode value,
				OutputCollector<Text, DoubleWritable> output, Reporter reporter)
						throws IOException {
			
			output.collect(key, value.getCurrentPageRank());
			
		}

	}
	
	/**
	 * Creates an instance of this tool.
	 */
	public MapReducePageRankWithMissingMass() {}

	/**
	 *  Prints argument options
	 * @return int
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
		
		//Must have four arguments
		if (args.length != 4) {
			MapReducePageRankWithMissingMass.printUsage();
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
		JobConf conf1 = new JobConf(MapReducePageRankWithMissingMass.class);
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
		
		int iterations = 218;
		String inputForPagerank = args[1];
		String outputPathToInitialPagerank = null;
		String outputPathToUpdatedPagerank = null;
		
		//Page Rank Calculation Job 2 (Iterative)
		for(int i = 0; i < iterations; i++) {
			
			sLogger.info("***** ITERATION " + i);
			
			outputPathToInitialPagerank = args[1] + i;
			
			//Pure Page Rank Calculation Job Setup
			JobConf pageRankJob = new JobConf(getConf(), MapReducePageRankWithMissingMass.class);
			
			FileInputFormat.setInputPaths(pageRankJob, new Path(inputForPagerank));
			FileOutputFormat.setOutputPath(pageRankJob, new Path(outputPathToInitialPagerank));
			FileOutputFormat.setCompressOutput(pageRankJob, false);
			
			pageRankJob.setJobName("PP Iteration " + i);
			pageRankJob.setNumMapTasks(mapTasks);
			pageRankJob.setNumReduceTasks(reduceTasks);
			
			pageRankJob.setOutputKeyClass(Text.class);
			pageRankJob.setOutputValueClass(MapReduceNode.class);
			
			pageRankJob.setOutputFormat(SequenceFileOutputFormat.class);
			pageRankJob.setInputFormat(SequenceFileInputFormat.class);
			
			pageRankJob.setMapperClass(MapReducePageRankWithMissingMass.PageRankCalcMapper.class);
			pageRankJob.setReducerClass(MapReducePageRankWithMissingMass.PageRankCalcReducer.class);
			pageRankJob.setPartitionerClass(HashPartitioner.class);
			
			// Delete the output directory if it exists already
			Path tempPageRankDir = new Path(outputPathToInitialPagerank);
			FileSystem.get(tempDir.toUri(), conf1).delete(tempPageRankDir, true);
			
			startTime = System.currentTimeMillis();
			
			//Run Pure Page Rank Calculation Job
			RunningJob runningJob = JobClient.runJob(pageRankJob);
			
			sLogger.info("PP Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0
					+ " seconds");
			
			//Delete the input directory if it exists already
			Path tempInputPageRankDir = new Path(inputForPagerank);
			FileSystem.get(tempDir.toUri(), conf1).delete(tempInputPageRankDir, true);
						
			//Update Page Rank Job
			outputPathToUpdatedPagerank = args[1]+"updatedPageRank"+i;

			//Update Page Rank Job Setup
			JobConf pageRankUpdateJob = new JobConf(getConf(), MapReducePageRankWithMissingMass.class);
			
			FileInputFormat.setInputPaths(pageRankUpdateJob, new Path(outputPathToInitialPagerank));
			FileOutputFormat.setOutputPath(pageRankUpdateJob, new Path(outputPathToUpdatedPagerank));
			FileOutputFormat.setCompressOutput(pageRankUpdateJob, false);
			
			pageRankUpdateJob.setJobName("UP Iteration " + i);
			pageRankUpdateJob.setNumMapTasks(mapTasks);
			pageRankUpdateJob.setNumReduceTasks(reduceTasks);
			
			pageRankUpdateJob.setOutputKeyClass(Text.class);
			pageRankUpdateJob.setOutputValueClass(MapReduceNode.class);
			
			pageRankUpdateJob.setOutputFormat(SequenceFileOutputFormat.class);
			pageRankUpdateJob.setInputFormat(SequenceFileInputFormat.class);
			
			pageRankUpdateJob.setMapperClass(MapReducePageRankWithMissingMass.UpdatePageRankMap.class);
			pageRankUpdateJob.setPartitionerClass(MapReducePageRankWithMissingMass.PageRankPartitioner.class);
			pageRankUpdateJob.setReducerClass(MapReducePageRankWithMissingMass.UpdatePageRankReducer.class);
			
			//Delete the output directory if it exists already
			Path tempUpdatedPageRankDir = new Path(outputPathToUpdatedPagerank);
			FileSystem.get(tempDir.toUri(), conf1).delete(tempUpdatedPageRankDir, true);
			
			startTime = System.currentTimeMillis();
			
			//Run Update Page Rank Job Setup
			RunningJob runningUpdateJob = JobClient.runJob(pageRankUpdateJob);
			
			sLogger.info("PP Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0
					+ " seconds");
			
			//Delete the input directory if it exists already
			Path tempOutputPageRankDir = new Path(outputPathToInitialPagerank);
			FileSystem.get(tempDir.toUri(), conf1).delete(tempOutputPageRankDir, true);
			
			//Set the output path of this iteration to be the inputpath for the next iteration
			inputForPagerank = outputPathToUpdatedPagerank;
		}
		
		//Output Job 3
		outputPathToUpdatedPagerank = args[1]+"FinalPageRank";
		
		//Output Job Setup
		JobConf outputJob = new JobConf(getConf(), MapReducePageRankWithMissingMass.class);
		
		FileInputFormat.setInputPaths(outputJob, new Path(inputForPagerank));
		FileOutputFormat.setOutputPath(outputJob, new Path(outputPathToUpdatedPagerank));
		FileOutputFormat.setCompressOutput(outputJob, false);
		
		outputJob.setJobName("Final Pagerank Output");
		outputJob.setNumMapTasks(mapTasks);
		outputJob.setNumReduceTasks(0);
		
		outputJob.setOutputKeyClass(Text.class);
		outputJob.setOutputValueClass(DoubleWritable.class);

		outputJob.setInputFormat(SequenceFileInputFormat.class);
		outputJob.setOutputFormat(TextOutputFormat.class);
		
		outputJob.setMapperClass(MapReducePageRankWithMissingMass.OutputMap.class);
		
		//Delete the output directory if it exists already
		Path tempUpdatedPageRankDir = new Path(outputPathToUpdatedPagerank);
		FileSystem.get(tempDir.toUri(), conf1).delete(tempUpdatedPageRankDir, true);
		
		startTime = System.currentTimeMillis();
		
		//Run Output Job
		RunningJob runningUpdateJob = JobClient.runJob(outputJob);
		
		sLogger.info("Final Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0
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

		/*Count the total number of documents in graph needed for redistributing
		total missing mass*/
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
        
		numberOfReducers = Integer.parseInt(args[3]);
		
		numberNodes = count;
		
		int res = ToolRunner.run(new Configuration(), new MapReducePageRankWithMissingMass(), args);
		System.exit(res);
	}

}
