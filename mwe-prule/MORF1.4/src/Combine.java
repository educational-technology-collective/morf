import java.io.*;
import java.text.*;
import java.util.*;

/**
 * 
 * @author Miguel Andres
 * @version 2.0
 * @date 18 Jun 2
 * Input: path to folder containing course-level output files
 * Output:CSV (Z-score,p,+,-,null,odds_mean,risk_mean)
 * CMD: java Combine "/Users/miguelandres/Documents/eclipse-workspace/MORF1.4/outputs/" "/Users/miguelandres/Documents/eclipse-workspace/MORF1.4/combine-outputCSV.csv"
 *
 */

public class Combine {
	public static void main(String args[]) throws Exception {
		String inputPATH = "";
		String outputCSV = "";
		if (args.length < 2) {
			// inputPATH = "outputs/";
			// outputCSV = "combine-outputCSV.csv";
			System.exit(0);
		}
		else {
			inputPATH = args[0];
			outputCSV = args[1];
		}
		// 1. Combine all course "Execute" outputs into a single HashMap
		// format of input: "00,01,10,11,chi_square,z,p,odds_ratio,risk_ratio"
		File inputFolder = new File(inputPATH);
		File[] inputFiles = inputFolder.listFiles();
		int numCourses = inputFiles.length;
		HashMap<String, Double[]> inputs = new HashMap<String, Double[]>();
		ArrayList<String> inputNames = new ArrayList<String>();
		PrintWriter out = new PrintWriter(outputCSV);
		out.println("Z-score,p,+,-,null,odds_mean,risk_mean");
		String output = "";
		NumberFormat formatter = new DecimalFormat("#0.000");
		
		for (int i = 0; i < numCourses; i++) {
			String inputName = inputFiles[i].getName();
			inputs.put(inputName, new Double[5]); // {direction,z,p,odds,risk}
			inputNames.add(inputName);
			
			Scanner in = new Scanner(inputFiles[i]);
			in.nextLine();
			StringTokenizer st = new StringTokenizer(in.nextLine(), ",");
			double percent0 = Double.parseDouble(st.nextToken()); 
			double percent1 = Double.parseDouble(st.nextToken());
			
			percent0 /= (percent0 + Double.parseDouble(st.nextToken())); // percentage of those who fell within rule
			percent1 /= (percent1 + Double.parseDouble(st.nextToken())); // percentage of those who didn't
			if (percent0 > percent1) inputs.get(inputName)[0] = 1.0;
			else inputs.get(inputName)[0] = -1.0;
			
			st.nextToken(); // skipped chi-square
			for (int j = 1; j < 5; j++) inputs.get(inputName)[j] = Double.parseDouble(st.nextToken());
			in.close();
		}
		
		// 2. Calculate cumulative Z-score: sum of all Zs /  Math.sqrt(num courses)
		double stouffer = 0.0;
		for (int i = 0; i < numCourses; i++) stouffer += inputs.get(inputNames.get(i))[1];
		stouffer /= Math.sqrt(numCourses);
		output = formatter.format(stouffer) + ",";
		if (stouffer > 5)
			output += "<0.001,";
		else output += getP(stouffer) + ",";
		System.out.println("1	COMPUTED Z-SCORE");
		
		// 3. Count number of courses rule and counterfactual replicated in, and number of nonsignificant replications
		int numPos = 0;
		int numNeg = 0;
		int numNull = 0;
		for (int i = 0; i < numCourses; i++) {
			double dir = inputs.get(inputNames.get(i))[0];
			double p = inputs.get(inputNames.get(i))[2];
			
			if (p < 0.05)
				if (dir == 1.0) numPos++;
				else numNeg++;
			else
				numNull++;
		}
		output += numPos + "," + numNeg + "," + numNull + ",";
		System.out.println("2	COUNTED NUM REPLICATIONS");
		
		// 4. Calculate mean of odds and risk ratios
		double oddsMean = 0.0;
		double riskMean = 0.0;
		for (int i = 0; i < numCourses; i++) {
			oddsMean += inputs.get(inputNames.get(i))[3];
			riskMean += inputs.get(inputNames.get(i))[4];
		}
		oddsMean /= numCourses;
		riskMean /= numCourses;
		
		output += formatter.format(oddsMean) + "," + formatter.format(riskMean);
		System.out.println("3	CALCULATED EFFECT SIZES");
		
		out.println(output);
		System.out.println(output);
		out.close();
	}
	
	public static double getP(double z) {
		return 0.0;
	}
}
