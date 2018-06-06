import java.io.*;
import java.text.*;
import java.util.*;
import jess.*;

/**
 * 
 * @author Miguel Andres
 * @version 2.0
 * @date 18 Apr 13
 * Input: path to all CSV files (student_id<type:String>,feature<type:Double>,output<type:Double>): student-level (i.e., 1 row per student)
 * Input: TXT (production rule)
 * Output:CSV (00,01,10,11,chi_square,p)
 * CMD: java -cp /Users/miguelandres/Documents/eclipse-workspace/MORF1.4/jars/*: Execute "/Users/miguelandres/Documents/eclipse-workspace/MORF1.4/inputCSV.csv" "/Users/miguelandres/Documents/eclipse-workspace/MORF1.4/inputTXT.txt" "/Users/miguelandres/Documents/eclipse-workspace/MORF1.4/outputCSV.csv"
 *
 */

public class Execute {
	public static void main(String[] args) throws Exception {
		String inputPath = "";
		String inputTXT = "";
		String outputCSV = "";
		if (args.length < 3) {
			inputPath = "execute-input";
			inputTXT = "inputTXT.txt";
			outputCSV = "outputs/outputCSV4.csv";
			// System.exit(0);	
		}
		else {
			inputPath = args[0];
			inputTXT = args[1];
			outputCSV = args[2];
		}
		
		File inputFolder = new File(inputPath);
		File[] temp = inputFolder.listFiles(); // takes care of ignoring all temp files automatically created during job
		ArrayList<File> inputs = new ArrayList<File>();
		for (int i = 0; i < temp.length; i++) {
			if (!(temp[i].getName().charAt(0) == '.'))
				inputs.add(temp[i]);
		}
		PrintWriter out = new PrintWriter(outputCSV);
		out.println("00,01,10,11,chi_square,z,p,odds_ratio,risk_ratio");
		
		for (int i = 0; i < inputs.size(); i++) {
			// 1. Store feature and output in HashMap<key:userID>
			ArrayList<String> users = new ArrayList<String>();
			HashMap<String, String[]> features = new HashMap<String, String[]>();
			
			Scanner in = new Scanner(inputs.get(i));
			while (in.hasNextLine()) {
				String s = in.nextLine();
				StringTokenizer st = new StringTokenizer(s, ",");
				String userID = st.nextToken();
				users.add(userID);
				features.put(userID, new String[2]);
				features.get(userID)[0] = st.nextToken();
				features.get(userID)[1] = st.nextToken();
			}
			in.close();
			
			// 2. Instantiate expert system
			Rete r = new Rete();
			r.addOutputRouter("t", out);
			r.eval("(watch all)");
			
			String template = "(deftemplate curr-feature (slot userID) (slot feature) (slot output))";
			r.eval(template);
			
			// 3a. Enter students into expert system
			double avgFeature = 0;
			for (int j = 0; j < users.size(); j++) {
				String userID = users.get(j);
				String feature = features.get(userID)[0];
				String output = features.get(userID)[1];
				String assertion = "(assert (curr-feature (userID \"" + userID + "\") (feature " + feature + ") (output " + output + ")))";
				r.eval(assertion);
				
				avgFeature += Double.parseDouble(feature);
			}
			
			// 3b. Compute and enter average into expert system
			avgFeature /= users.size();
			String assertion = "(defglobal ?*avg-feature* = " + avgFeature + ")";
			r.eval(assertion);
			
			// 3c. Enter production rule into expert system
			in = new Scanner(new File(inputTXT));
			while (in.hasNextLine())
				r.eval(in.nextLine());
			in.close();
			
			// 3d. Run expert system
			r.eval("(run)");
			
			// 4. Run chi-squared test
			double total0 = Double.parseDouble(r.eval("(return ?*i1*)").toString());
			double quad00 = Double.parseDouble(r.eval("(return ?*it1*)").toString());
			double quad10 = total0 - quad00;
			double total1 = Double.parseDouble(r.eval("(return ?*i-1*)").toString());
			double quad01 = Double.parseDouble(r.eval("(return ?*it-1*)").toString());
			double quad11 = total1 - quad01;
			double totalP = quad00 + quad01;
			double totalF = quad10 + quad11;
			double total = total0 + total1;
			
			double exp00 = total0 * totalP / total;
			double exp01 = total1 * totalP / total;
			double exp10 = total0 * totalF / total;
			double exp11 = total1 * totalF / total;
			
			double sq00 = Math.pow((quad00 - exp00),2) / exp00;
			double sq01 = Math.pow((quad01 - exp01),2) / exp01;
			double sq10 = Math.pow((quad10 - exp10),2) / exp10;
			double sq11 = Math.pow((quad11 - exp11),2) / exp11;
			
			double chiSq = sq00 + sq01 + sq10 + sq11;
			double p = pochisq(chiSq, 1);
			
			double odds = (quad00 * quad11) / (quad01 * quad10);
			double risk = (quad00 / total0) / (quad10 / total1);
			
			// 5. Print output
			NumberFormat formatter = new DecimalFormat("#0.000");
			out.println(quad00 + "," + quad01 + "," + quad10 + "," + quad11 + "," + formatter.format(chiSq) + "," + 
						formatter.format(Math.sqrt(chiSq)) + "," + formatter.format(p) + "," + formatter.format(odds) + "," + formatter.format(risk));
			
			System.out.println(quad00 + "," + quad01 + "," + totalP);
			System.out.println(quad10 + "," + quad11 + "," + totalF);
			System.out.println(total0 + "," + total1 + "," + total);
			System.out.println();
			System.out.println(exp00 + "," + exp01);
			System.out.println(exp10 + "," + exp11);
		}
		
		out.close();
	}
	
	private static final double LOG_SQRT_PI = Math.log(Math.sqrt(Math.PI));
    private static final double I_SQRT_PI = 1 / Math.sqrt(Math.PI);
    public static final int MAX_X = 20; // max value to represent exp(x)
 
   /* POCHISQ -- probability of chi-square value
        Adapted from:
        Hill, I. D. and Pike, M. C. Algorithm 299
        Collected Algorithms for the CACM 1967 p. 243
        Updated for rounding errors based on remark in
        ACM TOMS June 1985, page 185
    */
    public static double pochisq(double x, int df) {
        double a, s;
        double e, c, z;
 
        if (x <= 0.0 || df < 1) {
            return 1.0;
        }
        a = 0.5 * x;
        boolean even = (df & 1) == 0;
        double y = 0;
        if (df > 1) {
            y = ex(-a);
        }
        s = (even ? y : (2.0 * poz(-Math.sqrt(x))));
        if (df > 2) {
            x = 0.5 * (df - 1.0);
            z = (even ? 1.0 : 0.5);
            if (a > MAX_X) {
                e = (even ? 0.0 : LOG_SQRT_PI);
                c = Math.log(a);
                while (z <= x) {
                    e = Math.log(z) + e;
                    s += ex(c * z - a - e);
                    z += 1.0;
                }
                return s;
            } else {
                e = (even ? 1.0 : (I_SQRT_PI / Math.sqrt(a)));
                c = 0.0;
                while (z <= x) {
                    e = e * (a / z);
                    c = c + e;
                    z += 1.0;
                }
                return c * y + s;
            }
        } else {
            return s;
        }
    }
 
 
    public static double poz(double z) {
        double y, x, w;
        double Z_MAX = 6.0; // Maximum meaningful z value 
        if (z == 0.0) {
            x = 0.0;
        } else {
            y = 0.5 * Math.abs(z);
            if (y >= (Z_MAX * 0.5)) {
                x = 1.0;
            } else if (y < 1.0) {
                w = y * y;
                x = ((((((((0.000124818987 * w
                        - 0.001075204047) * w + 0.005198775019) * w
                        - 0.019198292004) * w + 0.059054035642) * w
                        - 0.151968751364) * w + 0.319152932694) * w
                        - 0.531923007300) * w + 0.797884560593) * y * 2.0;
            } else {
                y -= 2.0;
                x = (((((((((((((-0.000045255659 * y
                        + 0.000152529290) * y - 0.000019538132) * y
                        - 0.000676904986) * y + 0.001390604284) * y
                        - 0.000794620820) * y - 0.002034254874) * y
                        + 0.006549791214) * y - 0.010557625006) * y
                        + 0.011630447319) * y - 0.009279453341) * y
                        + 0.005353579108) * y - 0.002141268741) * y
                        + 0.000535310849) * y + 0.999936657524;
            }
        }
        return z > 0.0 ? ((x + 1.0) * 0.5) : ((1.0 - x) * 0.5);
    }
 
 
    public static double ex(double x) {
        return (x < -MAX_X) ? 0.0 : Math.exp(x);
    }
}
