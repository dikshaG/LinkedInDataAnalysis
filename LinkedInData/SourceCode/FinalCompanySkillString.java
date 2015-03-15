package IndependentStudy;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.spark.api.java.function.Function;

import scala.Tuple2;

/*
 * As the output is expected to be a string,
 * Reduced Company-Skill is converted to
 * String JavaRDD with count of Skills occurrence
 * each company, separating Company and Skills
 * by '|'.
 */

public class FinalCompanySkillString implements
		Function<Tuple2<String, String>, String> {

	public String call(Tuple2<String, String> CompanySkill) throws Exception {
		String Pair = "";
		String s = CompanySkill._2().toString();
		String sFinal = "";
		
		//Skills are stored in list and occurrence of each skill is counted.
		List<String> myList = Arrays.asList(s.split("\\s*,\\s*"));

		Set<String> unique = new HashSet<String>(myList);

		for (String key : unique) {
			if (!key.equals(""))
				sFinal += key + ": " + Collections.frequency(myList, key) + ",";
		}
		
		if (!sFinal.equals(""))
			Pair = CompanySkill._1().toString() + " | "
					+ sFinal.substring(0, sFinal.length() - 1);

		return Pair;
	}

}
