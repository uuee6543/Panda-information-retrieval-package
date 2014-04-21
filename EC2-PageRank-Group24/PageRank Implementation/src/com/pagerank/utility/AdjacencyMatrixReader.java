package com.pagerank.utility;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;

import com.pagerank.distributed.io.ArrayListWritableComparable;

/**
 * <p>Used for reading a matrix-like adjacency list representation</p>
 * 
 * <p>For local implementation of PageRank based on matrix <b>only</b></p>
 * @author group24
 *
 */
public class AdjacencyMatrixReader {
	
	/**
	 * path to the adjacency matrix file
	 */
	private File _path;
	
	/**
	 * constant for normalisation
	 */
	private static final double CONSTANT = 0.85;

	
	/**
	 * last used key list
	 */
	private List<Integer> lastKeyList;
	
	/**
	 * AdjacencyMatrixReader constructor
	 * 
	 * @param path_ File path to the adjacency matrix file
	 */
	public AdjacencyMatrixReader(File path_) {
		this._path = path_;
	}
	
	/**
	 * Returns the last used list of keys (documents) used as 
	 * rows/columns in the matrix given by the file specified in the 
	 * class constructor
	 * 
	 * @return last used list of documents in the order in which they 
	 * appear in a matrix 
	 * @deprecated This should be replaced by a better implementation
	 */
	public List<Integer> getLastKeyList() {
		return lastKeyList;
	}
	
	private Map<Integer,List<Integer>> readInitialMatrix() throws FileNotFoundException {
		Scanner reader = new Scanner(this._path);
		reader.useDelimiter("\\t");
		Map<Integer,List<Integer>> initialMatrix = new LinkedHashMap<Integer, List<Integer>>();
		
		while(reader.hasNextLine()) {
			StringTokenizer st = new StringTokenizer(reader.nextLine(), "\t");
			int key = Integer.parseInt(st.nextToken());
			List<Integer> values = new ArrayList<Integer>();
			while(st.hasMoreTokens()) {
				values.add(Integer.parseInt(st.nextToken()));
			}
			initialMatrix.put(key, values);
		}
		return initialMatrix;
	}
	
	private Map<Text, List<Text>> readInitialMatrixStrings() throws FileNotFoundException {
		Scanner reader = new Scanner(this._path);
		reader.useDelimiter("\\t");
		Map<Text,List<Text>> initialMatrix = new LinkedHashMap<Text, List<Text>>();
		
		while(reader.hasNextLine()) {
			StringTokenizer st = new StringTokenizer(reader.nextLine(), "\t");
			Text key = new Text(st.nextToken());
			List<Text> values = new ArrayList<Text>();
			while(st.hasMoreTokens()) {
				values.add(new Text(st.nextToken()));
			}
			initialMatrix.put(key, values);
		}
		return initialMatrix;
	}
	
	/**
	 * This method returns adjacency matrix and updates the list 
	 * of last used documents
	 * 
	 * @return Adjacency matrix of type Integer[][]
	 * @throws FileNotFoundException
	 */
	public Double[][] computeAdjacencyMatrix() throws FileNotFoundException {
		
		Map<Integer,List<Integer>> initialMatrix = readInitialMatrix();
		
		List<Integer> keys = new ArrayList<Integer>();
		keys.addAll(initialMatrix.keySet());
		int matrixSize = keys.size();
		Double[][] matrix = new Double[matrixSize][matrixSize];
		
		lastKeyList = keys;
		
		for(int i = 0; i < matrixSize; i++) {
			Integer key = keys.get(i);
			List<Integer> values = initialMatrix.get(key);
			for(int j = 0; j < matrixSize; j++ ) {
				if(values.contains(keys.get(j))) {
					matrix[i][j] = 1.0;
				} else {
					matrix[i][j] = 0.0;
				}
			}

		}
		
		
		return matrix;
	}

	public Map<Text, Node> computeNodeListString() throws FileNotFoundException {
		Map<Text, Node> nodeList = new LinkedHashMap<Text, Node>();
		Map<Text, List<Text>> initialMatrix = readInitialMatrixStrings();
		for(Text nodeID : initialMatrix.keySet()) {
			nodeList.put(nodeID, new Node(nodeID, initialMatrix.get(nodeID)));
		}
		
		Map<Text, Node> extraDanglingNodes = new LinkedHashMap<Text, Node>();
		//int highestNodeID = nodeList.size();
		for(Map.Entry<Text, Node> nodeEntry : nodeList.entrySet()) {
			Node node = nodeEntry.getValue();
			for(Text id : node.getOutlinkNodeIDs()) {
				if(nodeList.containsKey(id)) {
					nodeList.get(id).addIncomingLinkNodeID(nodeEntry.getKey());
				} else {
					System.out.println("id: " + id + " not in the list!");
					/*
					 * The block below fixes non-existent dangling node problem
					 * uncomment only if absolutely necessary 
					 */
					//extraDanglingNodes.put(id, new Node(id, new ArrayListWritableComparable<Text> ()));
				}
			}
		}
		
		for(Map.Entry<Text, Node> extraNode : extraDanglingNodes.entrySet()) {
			nodeList.put(extraNode.getKey(), extraNode.getValue());
		}
		
		
		return nodeList;
	}
		
	/**
	 * This method returns transition matrix normalized 
	 * using the given constant <b>CONSTANT</b>(by default 0.85) 
	 * and with equal probabilities assigning to random nodes. 
	 * The method also includes random jump factor of 
	 * <b>1-CONSTANT</b> distributed across remaining documents
	 *  
	 * @return Transition matrix of type Double[][]
	 * @throws FileNotFoundException
	 */
	public Double[][] computeTransitionMatrix() throws FileNotFoundException {
		
		Map<Integer,List<Integer>> initialMatrix = readInitialMatrix();

		
		Double[][] adjMatrix = computeAdjacencyMatrix();
		
		double teleportation = ((1-CONSTANT)/adjMatrix.length);
		int counter = 0;
		double danglingProbability = (double)1/initialMatrix.size();
		for(Integer docID : initialMatrix.keySet()){
			
			double outlinkNum = initialMatrix.get(docID).size();
			for(int m =0 ; m <adjMatrix.length; m++){
				if(outlinkNum != 0) {
					adjMatrix[counter][m] = (adjMatrix[counter][m]*(CONSTANT/outlinkNum)+ teleportation); 
				} else {
					adjMatrix[counter][m] = danglingProbability;
				}
			}
			counter++;
		}
		return adjMatrix;
	}
	
	
}
