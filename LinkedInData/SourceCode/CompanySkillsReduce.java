package IndependentStudy;
import org.apache.spark.api.java.function.Function2;

/*
 * In this class, The input JavaPairRDD Company-Skill is
 * traversed by key and with same company key, skills are 
 * merged into 1 string and returned.
 * It results in key-value pair.
 */

public class CompanySkillsReduce implements Function2<String, String, String> {
	public String call(String s1, String s2) {
		String s = "";
		if (!s1.isEmpty() && !s2.isEmpty() && !s1.equals("") && !s2.equals(""))
			s = s1 + "," + s2;

		return s;

	}
}
