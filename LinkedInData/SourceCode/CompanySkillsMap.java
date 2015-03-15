package IndependentStudy;

import java.util.Iterator;
import org.apache.spark.api.java.function.PairFunction;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import org.json.simple.parser.ParseException;

import scala.Tuple2;

/*
 * This class extracts Company name and associated skills from JSON data and
 * returns the Key -Value pair with Company as key and Skills as value.
 */

public class CompanySkillsMap implements PairFunction<String, String, String> {
	public Tuple2<String, String> call(String line) throws ParseException {

		String headline = "";
		String skillStr = "";
		String Company = "";

		JSONParser jsonParser = new JSONParser();
		JSONObject jsonObject = (JSONObject) jsonParser.parse(line);

		// get an 'experience' and 'skills' array from the JSON object
		JSONArray experience = (JSONArray) jsonObject.get("experience");
		JSONArray skills = (JSONArray) jsonObject.get("skills");

		// Skills are extracted and stored in variable names skillStr.

		if (skills != null) {
			for (int k = 0; k < skills.size(); k++) {
				if (skillStr == "")
					skillStr = skills.get(k) + ",";
				else {
					if (k == skills.size() - 1)
						skillStr += skills.get(k);
					else
						skillStr += skills.get(k) + ",";
				}
			}
		}

		// Company name is extracted and Stored in variable named 'Company'.

		if (experience != null) {

			// take first i.e. the most recent company value from the json array
			Iterator<JSONObject> i = experience.iterator();
			JSONObject innerObj = (JSONObject) i.next();
			JSONArray organization = (JSONArray) innerObj.get("organization");
			Iterator<JSONObject> companyNameIter = organization.iterator();
			JSONObject organizationObj = (JSONObject) companyNameIter.next();

			Company = organizationObj.get("name") + "";
		}

		/*
		 * If company field is not present, Company name is extracted from
		 * headline field if present only if skills are present too as we want
		 * company-skill pair.
		 */
		if ((Company == "" || Company.equalsIgnoreCase("null") || Company
				.isEmpty() == true)
				&& skillStr.isEmpty() == false
				&& skillStr != "") {
			headline = (String) jsonObject.get("headline");
			if (headline != null && !headline.matches("--")
					&& headline.contains(" at "))
				Company = headline.substring(headline.lastIndexOf(" at ") + 4);
		}

					
			/* If by any chance, either of the fields,its not included in final
			   output, a blank line is returned instead which will reduce to 1
			   single line after reduce.
			*/		
		if ((Company.isEmpty() || Company.equals("")) || (skillStr.isEmpty() || skillStr.equals("")))
			return new Tuple2<String, String>("","");
		
		else {
		// Pre-processing of company name
			Company = CompanyPreProcessing.CPP(Company);
			return new Tuple2<String, String>(Company, skillStr);
			}
			
			/* If a single blank line in the final output is unacceptable,
			   it can be substituted with following option. This will 
			   generate 2 records- 1 which has no company and all skills
			   and other companies with no skills.

		if ((Company.isEmpty() || Company.equals("")) && !(skillStr.isEmpty() || skillStr.equals("")))
			return new Tuple2<String, String>("No Company", skillStr);
		
		if (!(Company.isEmpty() || Company.equals("")) && (skillStr.isEmpty() || skillStr.equals("")))
			return new Tuple2<String, String>("No skills",Company);
			*/

	}
}
