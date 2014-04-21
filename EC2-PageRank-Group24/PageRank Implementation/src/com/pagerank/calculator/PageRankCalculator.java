package com.pagerank.calculator;

import java.util.Map;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;

import com.pagerank.utility.Node;

/**
 * <p><b>PageRankCalculator</b></p>
 * 
 * <p>Calculates Page Rank using the iterative approach locally.</p>
 *
 */
public class PageRankCalculator {
	
	/**
	 * Random jump factor to be factored into pageRank in the second MapReduce Job
	 */
	public static final Double RANDOM_JUMP_FACTOR = 0.85;
	
	/**
	 * <p>Calculate Page Rank - Local Implementation</p>
	 * 
	 * @param nodeMap Map<Text, Node>
	 * @param includeMissingMass boolean
	 * @return boolean
	 */
	public boolean calculatePagerank(Map<Text, Node> nodeMap, boolean includeMissingMass) {
		long currentTime = System.currentTimeMillis();
		
		Double missingPageRankMass = 0.0; //Stores total missing mass from all dangling nodes
		
		 /* For every document in the map if it is not a dangling node then set the contribution
		 * variable for this node by dividing it's current Page Rank by it's total number of outlinks
		 * 
		 * For every dangling node that has no outlinks add its current page rank to the missing mass.
		 * 
		 * Once all missing mass is stored, calculate the distribution given to each document by dividing the
		 * missing mass total by the total number of documents
		 * 
		 * This would be the first job in the MapReduce Framework.*/
		System.out.println("\tMAP: calc missing mass and node contribution....");
		
		for(Map.Entry<Text, Node> nodeEntry : nodeMap.entrySet()) {
			Node node = nodeEntry.getValue();
			if(node.getOutlinkNodeIDs().size() == 0) {
				if(includeMissingMass) {
					missingPageRankMass += node.getCurrentPageRank().get();
				} 
			} else {
				node.setContribution(new DoubleWritable(node.getCurrentPageRank().get()/node.getOutlinkNodeIDs().size()));
			}
		}
		if(includeMissingMass) {
			missingPageRankMass = missingPageRankMass / (nodeMap.size());
		}
		
		 /* For every document in the map calculate it's pure page rank by summing all of it's
		 * contributions from each of it's inlinks.
		 * 
		 * Add the pure page rank to the list of page ranks for this node.
		 * 
		 * This would be the second job in the MapReduce Framework which would be run 
		 * at each iteration.*/
		System.out.println("\tREDUCE: calculate pageRank for each node....");
		
		for(Map.Entry<Text, Node> nodeEntry : nodeMap.entrySet()) {
			Node node = nodeEntry.getValue();
			double pageRankContributionSum = 0.0;
			for(Text inLinkNodeID : node.getIncomingLinkNodeID()) {
				pageRankContributionSum += nodeMap.get(inLinkNodeID).getContribution().get();
			}
			double pageRankCalc = (pageRankContributionSum);
			node.addPagerank(new DoubleWritable(pageRankCalc));
		}
		
		 /* For every document in the map calculate the updated page rank score by factoring in
		 * the random jump factor and missing mass if it is being factored in.
		 * 
		 * This would be the third job in the MapReduce Framework which would be run 
		 * at each iteration.*/
		System.out.println("\tMAP: factor in missing mass and jump factor....");
		for(Map.Entry<Text, Node> nodeEntry : nodeMap.entrySet()) {
			Node node = nodeEntry.getValue();
			double pageRankNonDangling = node.getCurrentPageRank().get();
			if(includeMissingMass) {
				node.updateCurrentPageRank(new DoubleWritable((1-RANDOM_JUMP_FACTOR)/nodeMap.size() + (RANDOM_JUMP_FACTOR *(pageRankNonDangling + missingPageRankMass))));
			} else {
				node.updateCurrentPageRank(new DoubleWritable((1-RANDOM_JUMP_FACTOR)/nodeMap.size() + (RANDOM_JUMP_FACTOR *(pageRankNonDangling)))); 
			}	
		}
		
		 /* For every document in the map check for convergence
		 * 
		 * This would be the included in the iterations in MapReduce.*/
		for(Text nodeID : nodeMap.keySet()) {
			if(!nodeMap.get(nodeID).hasConverged()) {
				System.out.println("\t\tTime elapsed: " + (System.currentTimeMillis() - currentTime) /1000 + " seconds");
				return false;
			}
		}
		System.out.println("\t\tTime elapsed: " + (System.currentTimeMillis() - currentTime) /1000 + " seconds");
		return true;
	}
	
	/**
	 * <p>Calculate Page Rank - Local Implementation</p>
	 * 
	 * @param nodeMap Map<Text, Node>
	 * @return boolean
	 */
	public boolean calculatePagerank(Map<Text, Node> nodeMap) {
		return calculatePagerank(nodeMap, false);
	}
	
	/**
	 * <p>Normalise - Local Page Rank Values Implementation</p>
	 * 
	 * <p>If missing mass is not factored in after each iteration of calculating Page Rank,
	 * then you must normalise the Page Rank values after convergence</p>
	 * 
	 * @param nodeMap Map<Text, Node>
	 */
	public void normalize(Map<Text, Node> nodeMap) {
		
		double totalPagerank = 0.0;
		for(Map.Entry<Text, Node> entry : nodeMap.entrySet()) {
			totalPagerank += entry.getValue().getCurrentPageRank().get();
		}
		for(Map.Entry<Text, Node> entry : nodeMap.entrySet()) {
			entry.getValue().addPagerank(new DoubleWritable(entry.getValue().getCurrentPageRank().get()/totalPagerank));
		}
	}
}
