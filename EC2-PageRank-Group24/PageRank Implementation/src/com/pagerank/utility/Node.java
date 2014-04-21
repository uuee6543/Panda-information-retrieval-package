package com.pagerank.utility;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.BinaryComparable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import com.pagerank.distributed.io.ArrayListWritableComparable;

/**
 * <p>Node object used to represent documents from the web used for local
 * implementation of iterative Page Rank calculation.</p>
 *
 */
public class Node implements Comparable, WritableComparable {
	
	private List<DoubleWritable> _pageRank;
	private Text _nodeID;
	private List<Text> _outLinkNodeIDs;
	private List<Text> _inLinkNodeIDs;
	private DoubleWritable _contribution;

	
	//convergence parameters:
	public static final int MINIMUM_NUMBER_OF_INTERATIONS = 10;
	public static final int CONVERGENCE_CONDITION_BACKTRACK_NUMBER = 4;
	
	/** CONSTRUCTOR FOR FULL  NODE
	 * 
	 * @param pageRank_ List<DoubleWritable>
	 * @param nodeID_ Text
	 * @param outLinkNodeIDs_ List<Text>
	 */
	public Node(List<DoubleWritable> pageRank_, Text nodeID_, List<Text> outLinkNodeIDs_ ) {
		
		this._pageRank = pageRank_;
		this._nodeID = nodeID_;
		this._outLinkNodeIDs = outLinkNodeIDs_;
		this._inLinkNodeIDs = new ArrayListWritableComparable<Text>();
		this._contribution = new DoubleWritable(0.0);
		if(outLinkNodeIDs_.size() != 0) {
			this._contribution = new DoubleWritable( (double) (1/outLinkNodeIDs_.size()));
		}
	}
	
	/** CONSTRUCTOR FOR NODE
	 * 
	 * @param nodeID_ Text
	 * @param outLinkNodeIDs_ List<Text>
	 */
	public Node(Text nodeID_, List<Text> outLinkNodeIDs_) {
		
		this(new ArrayListWritableComparable<DoubleWritable>(), nodeID_, outLinkNodeIDs_);
	}
	
	/**
	 * <p>KEEP TRACK OF THE LAST 4 PAGERANKS TO CHECK FOR CONVERGENCE</p>
	 * <p>Adds <b>Page Rank</b> to the top of the list (in a stack-like
	 * manner</p>
	 * 
	 * @param pageRankScore_ DoubleWritable
	 */
	public void addPagerank(DoubleWritable pageRankScore_) {
		this._pageRank.add(0, pageRankScore_);
		int listSize = this._pageRank.size();
		
		/*
		 * This bit fixes an extremely large memory leak previosuly 
		 * causing the algorithm to stall for HOURS!
		 */
		if(listSize > CONVERGENCE_CONDITION_BACKTRACK_NUMBER) {
			this._pageRank.remove(listSize-1);
		}
	}
	
	/**
	 * <p>Add a incoming link to this document, where each incoming link is
	 * stored just by it's URL and not the full Node object for this document.</p>
	 * 
	 * 
	 * @param incomingLinkNodeID_ Text
	 */
	public void addIncomingLinkNodeID(Text incomingLinkNodeID_) {
		this._inLinkNodeIDs.add(incomingLinkNodeID_);
	}
	
	/**
	 * <p>SET THE CONTRIBUTION GIVEN TO THE NODES OUTLINKS</p>
	 * <p>Contribution should be equal to the node's current 
	 * page rank divided by the total number of the node's 
	 * outlinks</p>
	 * 
	 * 
	 * @param contribution_ DoubleWritable
	 */
	public void setContribution(DoubleWritable contribution_) {
		this._contribution = contribution_;
	}
	
	/**
	 * <p>UPDATE THE LATEST PAGE RANK</p>
	 * 
	 * @param d DoubleWritable
	 */
	public void updateCurrentPageRank(DoubleWritable d) {
		this._pageRank.remove(0);
		this._pageRank.add(0, d);
	}
	
	/**
	 * <p>Return the list of incoming links for this document.
	 * 
	 * @return List<Text>
	 */
	public List<Text> getIncomingLinkNodeID() {
		return this._inLinkNodeIDs;
	}
	
	/**
	 * <p>RETURN THE NODE ID</p>
	 * 
	 * @return Text
	 */
	public Text getNodeID() {
		return this._nodeID;
	}
	
	/**
	 * <p>RETURN NUMBER OF OUTLINKS</p>
	 * 
	 * @return int
	 */
	public int getNumbrOfOutlinks(){
		return this._outLinkNodeIDs.size();
	}
	
	/**
	 * <p>RETURN THE LIST OF OUTLINKS</p>
	 * <p>Only the Node ID for each outlink is stored not
	 * the entire Node object</p>
	 * 
	 * @return List<Text>
	 */
	public List<Text> getOutlinkNodeIDs() {
		return this._outLinkNodeIDs;
	}
	
	/**
	 * <p>GET THE MOST RECENT PAGE RANK</p>
	 * 
	 * <p>The most recent page rank is the first page rank value in the 
	 * node's list of page rank values</p>
	 * 
	 * @return DoubleWritable
	 */
	public DoubleWritable getCurrentPageRank() {
		return this._pageRank.get(0);
	}
	
	public DoubleWritable getContribution() {
		return this._contribution;
	}
	
	/**
	 * <p>CHECK IF THE NODE PAGE RANK HAS CONVERGED</p>
	 * <p>Check the last 4 page rank values stored and if they
	 * are all equal then we consider the page rank for this 
	 * node to have converged.</p>
	 * 
	 * @return boolean
	 */
	public boolean hasConverged() {
			
		if(this._pageRank.size() < CONVERGENCE_CONDITION_BACKTRACK_NUMBER) {
			return false;
		}
		
		DoubleWritable currentPagerank = this._pageRank.get(0);
		for(int i = 1; i < CONVERGENCE_CONDITION_BACKTRACK_NUMBER; i++) {
			if(currentPagerank.get() != this._pageRank.get(i).get()) {
				return false;
			}
		}
		
		return true;
	}

	/**
	 * <p>COMPARE NODES</p>
	 * <p>Compares the double value stored as the 
	 * current page rank for each node</p>
	 * 
	 */
	@Override
	public int compareTo(Object o) {
		Node objectNode = (Node) o;
		if(getCurrentPageRank().get() > objectNode.getCurrentPageRank().get() ) {
			return 1;
		} else if(getCurrentPageRank().get() == objectNode.getCurrentPageRank().get()) {
			return 0;
		} else { 
			return -1;
		}
	}
	
	/**
	 * SERIALIZE - not used in the local implementation.
	 * 
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		((ArrayListWritableComparable<DoubleWritable>) this._pageRank).write(out);
		this._nodeID.write(out);
		((ArrayListWritableComparable<Text>) this._outLinkNodeIDs).write(out);
		((ArrayListWritableComparable<Text>) this._inLinkNodeIDs).write(out);
		this._contribution.write(out);
	}

	/**
	 * DESERIALIZE  - not used in the local implementation.
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		((ArrayListWritableComparable<DoubleWritable>) this._pageRank).readFields(in);
		this._nodeID.readFields(in);
		((ArrayListWritableComparable<Text>) this._outLinkNodeIDs).readFields(in);
		((ArrayListWritableComparable<Text>) this._inLinkNodeIDs).readFields(in);
		this._contribution.readFields(in);
		
	}
}
