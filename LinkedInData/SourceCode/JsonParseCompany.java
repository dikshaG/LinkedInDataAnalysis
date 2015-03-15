package IndependentStudy;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.fs.FileUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/*
 * This is the file having main.
 */

public class JsonParseCompany {
	   
	public static void main(String[] args) {		
		
		//Initializing Spark Context and loading the JSON file containing raw data.
		SparkConf sparkConf = new SparkConf().setAppName("JsonParser").setMaster("local");
	    JavaSparkContext sc = new JavaSparkContext(sparkConf);
		JavaRDD<String> dataRDD=sc.textFile(args[0]); 
		sparkConf.set("spark.shuffle.consolidateFiles", "true");
				
		//Extracting Company name and associated skills, storing them as key value pair.
		JavaPairRDD<String,String> CompanySkillMap = dataRDD.mapToPair(new CompanySkillsMap());	
		
			
		//Company-Skill JavaPairRDD is reduced.
		CompanySkillMap = CompanySkillMap.reduceByKey(new CompanySkillsReduce());
		
		//Generates the final output string.
		JavaRDD<String> finalResult=CompanySkillMap.map(new FinalCompanySkillString());
		
		String path="Output";
		
		File tempfile=new File(path);
		
		if(tempfile.isDirectory())
			try {
				FileUtil.fullyDelete(tempfile);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	
		// Saves the output as textfile.
		//CompanySkillMap.saveAsTextFile(path);
		
		finalResult.saveAsTextFile(path);
	}

}