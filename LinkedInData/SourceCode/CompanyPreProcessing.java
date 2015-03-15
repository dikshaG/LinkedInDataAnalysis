package IndependentStudy;

/*
 * This class, pre-process the company field 
 * removing words like Inc, Corp, etc. So
 * that records refering to same company
 * falls under same company name.
 */

public class CompanyPreProcessing {

	public static String CPP(String Company) {

		Company = Company.replaceAll("[.,:;]", "");

		if (Company.contains("Inc"))
			Company = Company.replaceAll("\\s*\\bInc\\b\\s*", "");

		if (Company.contains("Corp"))
			Company = Company.replaceAll("\\s*\\bCorp\\b\\s*", "");

		if (Company.contains("Tech"))
			Company = Company.replaceAll("\\s*\\bTech\\b\\s*", "");

		if (Company.contains("Technology"))
			Company = Company.replaceAll("\\s*\\bTechnology\\b\\s*", "");

		if (Company.contains("Office"))
			Company = Company.replaceAll("\\s*\\bOffice\\b\\s*", "");

		return Company;

	}

}
