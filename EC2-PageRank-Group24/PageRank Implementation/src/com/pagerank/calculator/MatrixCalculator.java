package com.pagerank.calculator;

public class MatrixCalculator {
	
	/**
	 * MatrixCalculator constructor. Takes no arguments
	 */
	public MatrixCalculator() { }

	/**
	 * This method multiplies two matrices. One <b>always</b> has to specify 
	 * both dimensions, even for 1xn or nx1 matrices 
	 * (Double[1][n] or Double[n][1] correspondingly)
	 * 
	 * @param matrixA First matrix
	 * @param matrixB Second matrix
	 * @return Double[][] matrix
	 */
	public Double[][] multiply(Double[][] matrixA, Double[][] matrixB) {

		int aRows = matrixA.length;
        int aColumns = matrixA[0].length;
        int bRows = matrixB.length;
        int bColumns = matrixB[0].length;
        
        if (aColumns != bRows) {
            throw new IllegalArgumentException("A:Rows: " + aColumns + " did not match B:Columns " + bRows + ".");
        }
        
        Double[][] matrixC = new Double[aRows][bColumns];
        for (int i = 0; i < aRows; i++) {
            for (int j = 0; j < bColumns; j++) {
            	matrixC[i][j] = 0.00000;
            }
        }

        for (int i = 0; i < aRows; i++) { // aRow
            for (int j = 0; j < bColumns; j++) { // bColumn
                for (int k = 0; k < aColumns; k++) { // aColumn
                	matrixC[i][j] += matrixA[i][k] * matrixB[k][j];
                }
            }
        }
        
        System.out.println("result of multiplication:");
        for(int i = 0; i < bColumns; i++) {
        	System.out.println("matrix[1][" + i + "]= " + matrixC[0][i]);
        }
        
        System.out.println("end of multiplication");
        
		return matrixC;
	}
	
}


