package de.uni_mannheim.informatik.wdi.datafusion.usecase.books.evaluation;

import de.uni_mannheim.informatik.wdi.datafusion.evaluation.EvaluationRule;
import de.uni_mannheim.informatik.wdi.datafusion.usecase.books.FusableBooks;
import de.uni_mannheim.informatik.wdi.identityresolution.similarity.SimilarityMeasure;
import de.uni_mannheim.informatik.wdi.identityresolution.similarity.string.TokenizingJaccardSimilarity;

public class BookTitleEvaluationRule extends EvaluationRule<FusableBooks> {
	SimilarityMeasure<String> sim = new TokenizingJaccardSimilarity();

	public boolean isEqual(FusableBooks record1, FusableBooks record2) {
		// the title is correct if all tokens are there, but the order does not matter
		return sim.calculate(record1.getBookName(), record2.getBookName()) == 1.0;
	}
}
