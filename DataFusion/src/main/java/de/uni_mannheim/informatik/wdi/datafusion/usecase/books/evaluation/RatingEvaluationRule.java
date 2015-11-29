package de.uni_mannheim.informatik.wdi.datafusion.usecase.books.evaluation;

import de.uni_mannheim.informatik.wdi.datafusion.evaluation.EvaluationRule;
import de.uni_mannheim.informatik.wdi.datafusion.usecase.books.FusableBooks;

public class RatingEvaluationRule  extends EvaluationRule<FusableBooks>{
	public boolean isEqual(FusableBooks record1, FusableBooks record2) {
		// the title is correct if all tokens are there, but the order does not matter
		double rating1= Math.round(record1.getRating());
		double rating2= Math.round(record2.getRating()/2); //divided by two to normalize thats why teh freiburg must go to the 2nd slot because it starts from 10
		
		
		if (rating1==rating2)
		{
			
			return true;
			
		}
		
		else {
			return false;
		}
	}
}
