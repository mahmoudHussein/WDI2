package de.uni_mannheim.informatik.wdi.datafusion.usecase.books;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import org.w3c.dom.Node;

import de.uni_mannheim.informatik.wdi.MatchableFactory;
import de.uni_mannheim.informatik.wdi.datafusion.FusableFactory;
import de.uni_mannheim.informatik.wdi.datafusion.RecordGroup;
import de.uni_mannheim.informatik.wdi.usecase.books.Authors;
import de.uni_mannheim.informatik.wdi.usecase.books.AuthorsFactory;

public class FusableBooksFactory extends MatchableFactory<FusableBooks> implements FusableFactory<FusableBooks> {

	@Override
	public FusableBooks createModelFromElement(Node node, String provenanceInfo) {
		String id = getValueFromChildElement(node, "Id");
		
		// create the object with id and provenance information
		FusableBooks book = new FusableBooks(id, provenanceInfo);
		
		// fill the attributes
		String ISBN = getValueFromChildElement(node, "ISBN");
		if(ISBN != null){
			book.setISBN(ISBN.replaceAll("\\(.*?\\)", "").replaceAll("[^\\dxX]",""));
		}
		
		String title = getValueFromChildElement(node, "Book_Name");
		if(title!=null) {
			book.setBookName(title.replaceAll("\\s*\\([^\\)]*\\)\\s*", " ").toLowerCase());
		}
		
		String publisher = getValueFromChildElement(node, "Publisher");
		if(publisher!=null){
			book.setPublisher(publisher.replaceAll("\\s*\\([^\\)]*\\)\\s*", " ").toLowerCase());
		}
		
		book.setGenre(getValueFromChildElement(node, "Genre"));
		book.setPages(getValueFromChildElement(node, "Pages"));
		book.setPubCountry(getValueFromChildElement(node, "Publication_Country"));
		
		
		String rating = getValueFromChildElement(node, "Rating");
		if(rating!= null){
		double ratingVal = (double) Double.parseDouble(rating);
		book.setRating(ratingVal);
		}
		book.setYear(getValueFromChildElement(node, "Publication_Date"));
		
		// load the list of authors
		List<Authors> authors = getObjectListFromChildElement(node, "Authors", "Author", new AuthorsFactory(), provenanceInfo);
		book.setAuthor(authors);
		
		return book;
	}

	@Override
	public FusableBooks createInstanceForFusion(RecordGroup<FusableBooks> cluster) {
		
		List<String> ids = new LinkedList<>();
		
		for(FusableBooks b : cluster.getRecords()) {
			ids.add(b.getIdentifier());
		}
		
		Collections.sort(ids);
		
		String mergedId = StringUtils.join(ids, '+');
		
		return new FusableBooks(mergedId, "fused");
	}
	
}
