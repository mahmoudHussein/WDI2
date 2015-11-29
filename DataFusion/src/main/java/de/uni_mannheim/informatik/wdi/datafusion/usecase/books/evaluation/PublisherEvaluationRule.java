package de.uni_mannheim.informatik.wdi.datafusion.usecase.books.evaluation;

import de.uni_mannheim.informatik.wdi.datafusion.evaluation.EvaluationRule;
import de.uni_mannheim.informatik.wdi.datafusion.usecase.books.FusableBooks;
import de.uni_mannheim.informatik.wdi.identityresolution.similarity.SimilarityMeasure;
import de.uni_mannheim.informatik.wdi.identityresolution.similarity.string.LevenshteinSimilarity;


	public class PublisherEvaluationRule extends EvaluationRule<FusableBooks> {
		SimilarityMeasure<String> sim = new LevenshteinSimilarity();

		public boolean isEqual(FusableBooks record1, FusableBooks record2) {
			// the title is correct if all tokens are there, but the order does not matter
				
			double similarity = sim.calculate(record1.getPublisher(), record2.getPublisher());
			
			if (similarity>=0.8){
					
					
					return true;
				}
				
				else
				{
					
					return false;
				}
		}
	}


