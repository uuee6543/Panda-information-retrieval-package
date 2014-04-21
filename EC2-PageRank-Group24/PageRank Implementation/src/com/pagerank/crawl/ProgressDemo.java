package com.pagerank.crawl;

import java.text.DecimalFormat;

public class ProgressDemo {
	
	static void updateProgress(double progressPercentage) {
		final int width = 100; // progress bar width in chars

		System.out.print("\r[");
		int i = 0;
		for (; i <= (int)(progressPercentage*width); i++) {
			System.out.print("=");
		}
		for (; i < width; i++) {
			System.out.print(" ");
		}
		System.out.print("] ("  + ((Math.round( progressPercentage * 10000.0 ) / 100.0) + 1) + "%)");
	}

	public static void main(String[] args) {
		try {
			for (double progressPercentage = 0.0; progressPercentage <= 1.0; progressPercentage += 0.01) {
				updateProgress(progressPercentage);
				Thread.sleep(20);
			}
		} catch (InterruptedException e) {}
	}
}