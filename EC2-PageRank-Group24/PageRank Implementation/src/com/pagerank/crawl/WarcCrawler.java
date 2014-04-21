package com.pagerank.crawl;

import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.zip.GZIPInputStream;

import org.apache.hadoop.io.Text;

/**
 * <p>Provides tools for sequentially crawling WARC files, 
 * creating an adjacency list file</p>
 * 
 * <p>Provides additional functionality of appending 
 * nonexistent nodes as dangling nodes in the adjacency file</p> 
 * @author group24
 *
 */
public class WarcCrawler {
	
	/**
	 * <p>Specify your own path do the Derby database</p>
	 * <p>Provide the following information:</p>
	 * <p>jdbc:derby:/Users/_username_/_database_folder_;create=true;user=_username_;password=_password_</p>
	 * <p> with the correct _password_, _username_ and _database_folder_
	 * 
	 */
	private static String dbURL = "jdbc:derby:/Users/<username>/<database_folder>;create=true;user=<username>;password=<password>";
	private static String tableName = "IRDM.OURMAPPINGS";
	
	/**
	 * Java JDBC connection to the Derby database
	 */
    private static Connection conn = null;
    private static PreparedStatement stmt = null;
    
    private static void createConnection()
    {
        try
        {
            Class.forName("org.apache.derby.jdbc.EmbeddedDriver").newInstance();
            conn = DriverManager.getConnection(dbURL); 
        }
        catch (Exception except)
        {
            except.printStackTrace();
        }
    }

    
    private static void insertMapping(String link, int docID)
    {
        try
        {
        	String query = "INSERT INTO " + tableName + " (link, docID) VALUES (? , ?)";
        	stmt = conn.prepareStatement(query);
        	stmt.setString(1, link);
        	stmt.setInt(2, docID);
            stmt.execute();
            stmt.close();
        }
        catch (SQLException sqlExcept)
        {
        	System.out.println("Error starts here: ");
        	System.out.println("Link is: " + link + "\nsize: " + link.length());
        	System.out.println("DocID is: " + docID);
            sqlExcept.printStackTrace();
            System.out.println("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx");
        }
    }
    
    private static void selectDocID(String linkString, IntPair counterDocID)
    {
        try
        {
        	String selectQuery = "select DOCID from " + tableName + " WHERE link = ?";
            stmt = conn.prepareStatement(selectQuery);
            stmt.setString(1, linkString);
            ResultSet results = stmt.executeQuery();
            
            if(!results.next()) {
            	results.close();
            	stmt.close();
            	int newDocID = counterDocID.highestDocID + 1;
            	counterDocID.highestDocID = newDocID;
            	counterDocID.currentDocID = newDocID;
            	insertMapping(linkString, newDocID);
            } else {
            	int docID = results.getInt(1);
            	results.close();
                stmt.close();
                counterDocID.currentDocID = docID;
            }
        }
        catch (SQLException sqlExcept)
        {
            sqlExcept.printStackTrace();
        }
    }
    
    private static void shutdown()
    {
        try
        {
            if (stmt != null)
            {
                stmt.close();
            }
            if (conn != null)
            {
                DriverManager.getConnection(dbURL + ";shutdown=true");
                conn.close();
            }           
        }
        catch (SQLException sqlExcept)
        {
            
        }
    }

    
    /**
     * <p>This method creates an adjacency file by reading a directory 
     * and traversing all gzipped WARC files in that directory. For 
     * each WARC file, it reads it sequentially and extracts all hyperlinks</p>
     * 
     * <p>It then creates an adjacency file in the following way:</p>
     * @param warcFileDirectory
     * @param outPutFileName
     * @return
     * @throws IOException
     */
    public int createAdjacencyFileWithoutDanglingNodes(File warcFileDirectory, String outPutFileName) throws IOException {
    	long startTime = System.currentTimeMillis();
    	
    	System.out.println("Creating adjacency file...");
    	System.out.println("Reading files from directory " + warcFileDirectory.toString() + 
    						", estimated number of warc files: " + warcFileDirectory.listFiles().length);
    	
    	File adjacencyFile = new File(outPutFileName);
    	
		if(!adjacencyFile.exists()) {
			adjacencyFile.createNewFile();
		} else {
			adjacencyFile.delete();
			adjacencyFile.createNewFile();
		}
		GZIPInputStream warcInputStream;
		DataInputStream warcDataInputStream;
		WarcRecord thisWarcRecord;
		BufferedWriter bwNewAdjacency;
		Map<String, Set<String>> adjacencyList;
		int counter = 0;
    	for(File warcFile : warcFileDirectory.listFiles()) {
    		if(warcFile.getName().matches("([0-9])*\\.warc\\.gz")) {
	    		System.out.println("Crawling file " + warcFile.getName() + "....");
				
				warcInputStream = new GZIPInputStream(new FileInputStream(warcFile));
				warcDataInputStream = new DataInputStream(warcInputStream);
				bwNewAdjacency = new BufferedWriter(new OutputStreamWriter (new FileOutputStream(outPutFileName, true), "UTF-8"));
				
				adjacencyList = new LinkedHashMap<String, Set<String>>();
				Set<String> outLinks;
				
				WarcHTMLResponseRecord htmlRecord;
				while((thisWarcRecord = WarcRecord.readNextWarcRecord(warcDataInputStream)) != null) {
					// see if it's a response record
					if(thisWarcRecord.getHeaderRecordType().equals("response")) {
		
						String URI = thisWarcRecord.getHeaderMetadataItem("WARC-Target-URI");
						htmlRecord = new WarcHTMLResponseRecord(thisWarcRecord);
		
						outLinks = new LinkedHashSet<String>();
						for(String linkString : htmlRecord.getURLOutlinks()) {
								outLinks.add(linkString);
						}
						
						adjacencyList.put(URI, outLinks);
						counter++;
						if(counter % 100 == 0) {
							System.out.println("Processed " + counter + " rows so far...");
							if(counter % 5000 == 0) {
								System.out.println("Writing intermediate file.....");
								for(Map.Entry<String, Set<String>> entry : adjacencyList.entrySet()) {
									bwNewAdjacency.write(entry.getKey().toString() + "\t");
									for(String link : entry.getValue()) {
										bwNewAdjacency.write(link + "\t");
									}
									bwNewAdjacency.newLine();
								}
								adjacencyList.clear();
							}
						}
					}
					
				}
				System.out.println("Writing intermediate file (final iteration of phase one - closing file).....");
				for(Map.Entry<String, Set<String>> entry : adjacencyList.entrySet()) {
					bwNewAdjacency.write(entry.getKey().toString() + "\t");
					for(String link : entry.getValue()) {
						bwNewAdjacency.write(link + "\t");
					}
					bwNewAdjacency.newLine();
				}
				adjacencyList.clear();
				bwNewAdjacency.close();
    		} 
    	}
    	
		System.out.println("SUCCESS!!!");
		System.out.println("Crawling took " + (double) (System.currentTimeMillis() - startTime)/60000 + " minutes (" + 
				(System.currentTimeMillis() - startTime)/1000 + " seconds)");
		
		return 1; 
    }
    
    /**
     * <p>This method traverses through an adjacency file and adds dangling nodes
     * at the <b>bottom</b> of the file <b>if and only if</b> they do not already
     * exist in the adjacency file as keys (first entries on any given line)
     * @param adjacencyFile actual adjancency file
     * @param sanityCheckOn boolean, if on then performs a full sanity check 
     * to perform the operation was executed correctly 
     * @return integer 1 (successful execution)
     */
    public int addDanglingNodes(File adjacencyFile, boolean sanityCheckOn){
    	System.out.println("STAGE 2 - ADDING DANGLING NODES.....");
    	
    	int counter = 0;
    	
		InputStreamReader frNewAdjacency = null;
		try {
			frNewAdjacency = new InputStreamReader(new FileInputStream(adjacencyFile), "UTF-8");
		} catch (UnsupportedEncodingException e2) {
			e2.printStackTrace();
		} catch (FileNotFoundException e2) {
			e2.printStackTrace();
		}
		Scanner reader = new Scanner(frNewAdjacency);
		

		System.out.println("Building a set of existing nodes...");
		Set<String> nodeList = new LinkedHashSet<String>();

		StringTokenizer st;
		String key;
		while(reader.hasNextLine()) {
			st = new StringTokenizer(reader.nextLine(), "\t");
			key = st.nextToken();
			nodeList.add(key);
		}
		
		reader.close();

		System.out.println("Creating dangling nodes set...");

		try {
			frNewAdjacency = new InputStreamReader(new FileInputStream(adjacencyFile), "UTF-8");
		} catch (UnsupportedEncodingException e1) {
			e1.printStackTrace();
		} catch (FileNotFoundException e1) {
			e1.printStackTrace();
		}
		reader = new Scanner(frNewAdjacency);
		
		
		Set<String> stringsToAdd = new LinkedHashSet<String>();
		String token;
		while(reader.hasNextLine()) {
			st = new StringTokenizer(reader.nextLine(), "\t");
			key = st.nextToken();
			while(st.hasMoreTokens()) {
				token = st.nextToken();
				if(!nodeList.contains(token)) {
					stringsToAdd.add(token);
					nodeList.add(token);
					if(counter % 50000 == 0) {
						System.out.println("Detected " + counter + " danging nodes...");
					}
					counter ++;
				} 
			}
		}

		reader.close();

		BufferedWriter bwNewAdjacency = null;
		try {
			bwNewAdjacency = new BufferedWriter(new OutputStreamWriter (new FileOutputStream(adjacencyFile, true), "UTF-8") );
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}

		counter = 0;
		System.out.println("Appending dangling nodes to the file...");
		PrintWriter out = new PrintWriter(bwNewAdjacency);

		for(String url : stringsToAdd) {
			counter++;
			out.println(url.toString());
			if(counter % 50000 == 0) {
				System.out.println("Appended " + counter + " dangling nodes...");
			}
		}
		System.out.println("Appended " + counter + " dangling nodes...");
		out.close();
		stringsToAdd.clear();
		
		try {
			frNewAdjacency = new InputStreamReader(new FileInputStream(adjacencyFile), "UTF-8");
		} catch (UnsupportedEncodingException e2) {
			e2.printStackTrace();
		} catch (FileNotFoundException e2) {
			e2.printStackTrace();
		}
		
		if(sanityCheckOn) {
			System.out.println("*********** Sanity check ***********");
			reader = new Scanner(frNewAdjacency);
			counter = 0;
			while(reader.hasNextLine()) {
				counter++;
				if(counter % 3000 == 0 ) {
					System.out.println("Sanity check: " + counter + " nodes checked!");
				}
				st = new StringTokenizer(reader.nextLine(), "\t");
				while(st.hasMoreTokens()) {
					key = st.nextToken();
					if(!nodeList.contains(key)){
						System.out.println();
					}
				}
			}
			reader.close();
		}
		nodeList.clear();
		
		System.out.println("Successfully added dangling nodes!!!!!......");
    	return 1;
    }

	/**
	 * <p>Helper class representing Integer pairs</p>
	 * <p>To be used in the database implementation of crawling</p>
	 * @author kamilprzekwas
	 *
	 */
	static class IntPair {
		int currentDocID;
		int highestDocID;
		
		public IntPair(int currentCounter_, int highestDocID_) {
			this.currentDocID = currentCounter_;
			this.highestDocID = highestDocID_;
		}
		
		public void setCounter(int newCounter) {
			this.currentDocID = newCounter;
		}
		
		public void setID(int id) {
			this.highestDocID = id;
		}
	}
}
