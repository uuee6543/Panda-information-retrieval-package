package com.pagerank.rank;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Comparator;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;

import com.pagerank.calculator.PageRankCalculator;
import com.pagerank.utility.AdjacencyMatrixReader;
import com.pagerank.utility.Node;

public class PageRank {

	public static void main(String[] args) {
		
		/*
		File file = new File("sample-tiny.txt");
		AdjacencyMatrixReader reader = new AdjacencyMatrixReader(file);
		Double[][] matrix = null;
		try {
			matrix = reader.computeTransitionMatrix();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		if(matrix != null) {
			
			int length = matrix[0].length;
			Double uniformPagerank = (double) 1/length;
			Double[][] pageRank = new Double[1][length];
			for(int i = 0; i < length; i++) { 
				pageRank[0][i] = uniformPagerank;
				System.out.println("matrix[1][" + i + "]=" + pageRank[0][i]);
			}
			
			MatrixCalculator calculator = new MatrixCalculator();
			for(int i = 0; i <= 12; i++) { 
				pageRank = calculator.multiply(pageRank, matrix);
			}
			
			List<Integer> keysUsed = reader.getLastKeyList();
			Map<Integer,Double> pageRankMap = new TreeMap<Integer,Double>();
			
			int counter = 0;
			for(Integer key : keysUsed) {
				pageRankMap.put(key, pageRank[0][counter]);
				counter++;
			}
			SortedSet<Map.Entry<Integer, Double>> pageRankSorted = entriesSortedByValues(pageRankMap);
			
			System.out.println("PageRank of nodes, in descending order:");
			for(Map.Entry<Integer, Double> entry : pageRankSorted) {
				System.out.println(entry.getValue() + "\t" + entry.getKey());
			}
		}
		*/
		long currentTime = System.currentTimeMillis();
		File file = new File("outputTwoNewWarc.txt");
		AdjacencyMatrixReader reader = new AdjacencyMatrixReader(file);
		try {
			Map<Text, Node> documentList = reader.computeNodeListString();
			double initialPagerank = (double) 1/documentList.size();
			for(Text documentID : documentList.keySet()) {
				Node node = documentList.get(documentID);
				node.addPagerank(new DoubleWritable(initialPagerank));
			}
			PageRankCalculator calculator = new PageRankCalculator();
			boolean hasConverged = false;
			int counter = 0;
			while(!hasConverged) {
				counter++;
				System.out.println("Starting iteration " + counter +"...");
				hasConverged = calculator.calculatePagerank(documentList, false);
			}
			System.out.println("Normalizing pagerank...");
			calculator.normalize(documentList);
			@SuppressWarnings(value = "unchecked")
			SortedSet<Map.Entry<Integer, Node>> pageRankSorted = entriesSortedByValues(documentList);
			//System.out.println("PageRank of nodes, in descending order:");
			
			
			File outputFile = new File("outputNewNewNEwWarc.txt");
			if(!outputFile.exists()) {
				outputFile.createNewFile();
			} else {
				outputFile.delete();
				outputFile.createNewFile();
			}
			FileWriter fwNewOutput = new FileWriter(outputFile.getAbsoluteFile());
			BufferedWriter bwNewOutput = new BufferedWriter(fwNewOutput);
			
			bwNewOutput.write("PageRank of nodes, in descending order:\n");
			for(Map.Entry<Integer, Node> entry : pageRankSorted) {
				bwNewOutput.write(entry.getValue().getCurrentPageRank() + "\t" + entry.getValue().getNodeID() + "\n");
				//System.out.println(entry.getValue().getCurrentPageRank() + "\t" + entry.getValue().getNodeID());
			}
			
			long secondsTaken = (System.currentTimeMillis() - currentTime)/1000;
			int minutesTaken = (int) secondsTaken/60;
			System.out.println("SUCCESS!\n" + 
							"Time taken: " + minutesTaken  +" minutes " + (secondsTaken - (minutesTaken * 60)) + " seconds!");
			bwNewOutput.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	static <K,V extends Comparable<? super V>>
	SortedSet<Map.Entry<K,V>> entriesSortedByValues(Map<K,V> map) {
	    SortedSet<Map.Entry<K,V>> sortedEntries = new TreeSet<Map.Entry<K,V>>(
	        new Comparator<Map.Entry<K,V>>() {
	            @Override public int compare(Map.Entry<K,V> e1, Map.Entry<K,V> e2) {
	            	// return e2.getValue().compareTo(e1.getValue()); - descending order
	            	 //return e1.getValue().compareTo(e2.getValue()); - ascending order
	                return e2.getValue().compareTo(e1.getValue()) == 0 ? 1 : e2.getValue().compareTo(e1.getValue());
	            }
	        }
	    );
	    sortedEntries.addAll(map.entrySet());
	    return sortedEntries;
	}
}
