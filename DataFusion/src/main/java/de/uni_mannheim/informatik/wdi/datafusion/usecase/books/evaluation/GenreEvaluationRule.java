package de.uni_mannheim.informatik.wdi.datafusion.usecase.books.evaluation;

import de.uni_mannheim.informatik.wdi.datafusion.evaluation.EvaluationRule;
import de.uni_mannheim.informatik.wdi.datafusion.usecase.books.FusableBooks;

public class GenreEvaluationRule extends EvaluationRule<FusableBooks>{

	public boolean isEqual(FusableBooks record1, FusableBooks record2) {
		// the title is correct if all tokens are there, but the order does not matter
		return record1.getGenre()==record2.getGenre();
	}
}
