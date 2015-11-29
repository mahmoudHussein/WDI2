package de.uni_mannheim.informatik.wdi.datafusion.usecase.books.evaluation;

import java.util.HashSet;
import java.util.Set;

import de.uni_mannheim.informatik.wdi.datafusion.evaluation.EvaluationRule;
import de.uni_mannheim.informatik.wdi.datafusion.usecase.books.FusableBooks;
import de.uni_mannheim.informatik.wdi.usecase.books.Authors;

public class AuthorsEvaluationRule extends EvaluationRule<FusableBooks> {

	
	
	public boolean isEqual(FusableBooks record1, FusableBooks record2) {
		Set<String> authors1 = new HashSet<>();
		
		for(Authors a : record1.getAuthors()) {
		
			authors1.add(a.getAuthorName());
		}
		
		Set<String> authors2 = new HashSet<>();
		for(Authors a : record2.getAuthors()) {
			authors2.add(a.getAuthorName());
		}
		
		return authors1.containsAll(authors2) && authors2.containsAll(authors1);
	}
}
