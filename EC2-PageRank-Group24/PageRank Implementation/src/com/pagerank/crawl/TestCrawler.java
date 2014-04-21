package com.pagerank.crawl;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;

public class TestCrawler {

	public static void main(String[] args) {
		long startTime = System.currentTimeMillis();
		
		if(args.length != 3) {
			System.out.println("usage: [input-dir] [output-file-path] [full|crawl|addnodes|sanity]");
			System.exit(-1);
		}
		if(!(args[2].equalsIgnoreCase("full") || args[2].equalsIgnoreCase("crawl") || args[2].equalsIgnoreCase("addnodes"))) {
			System.out.println("args[2] : full|crawl|addnodes");
			System.exit(-1);
		}
		String inputDirecotry = args[0];
		String outputFile = args[1];
		WarcCrawler cralwer = new WarcCrawler();
		File warcFileDir = new File(inputDirecotry);
		if(args[2].equalsIgnoreCase("full") || args[2].equalsIgnoreCase("crawl")) {
			try {
				cralwer.createAdjacencyFileWithoutDanglingNodes(warcFileDir, outputFile);
				//FileUtils.copyFile(new File(outputFile), new File(outputFile + "_temp"));
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		if(args[2].equalsIgnoreCase("addnodes") || args[2].equalsIgnoreCase("full")) {
			cralwer.addDanglingNodes(new File(outputFile), false);
		}
		else if(args[2].equalsIgnoreCase("sanity")) {
			
		}
		
		long currentTime = System.currentTimeMillis();
		int minutes = (int) (currentTime - startTime)/60000;
		int seconds = (int) ((double)(currentTime - startTime)/1000 - (minutes * 60));
		System.out.println("Task " + args[2] +" completed in " + minutes + " minutes " + seconds + " seconds"
							);
	}

}
