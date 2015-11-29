package de.uni_mannheim.informatik.wdi.datafusion.usecase.books;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import de.uni_mannheim.informatik.wdi.datafusion.Fusable;
import de.uni_mannheim.informatik.wdi.usecase.books.Authors;
import de.uni_mannheim.informatik.wdi.usecase.books.Books;

public class FusableBooks extends Books implements Fusable {

	public static final String ISBN = "ISBN";
	public static final String BOOK_NAME = "Book_Name";
	public static final String AUTHORS = "Authors";
	public static final String PUBLISHER = "Publisher";
	public static final String GENRE = "Genre";
	public static final String PAGES = "Pages";
	public static final String PUBCOUNTRY = "Publication_Country";
	public static final String PUBYEAR = "Publication_Date";
	public static final String RATING = "Rating";
	
	private Map<String, Collection<String>> provenance = new HashMap<>();
	
	public void setRecordProvenance(Collection<String> provenance) {
		this.provenance.put("RECORD", provenance);
	}
	public Collection<String> getRecordProvenance() {
		return provenance.get("RECORD");
	}
	
	public void setAttributeProvenance(String attribute, Collection<String> provenance) {
		this.provenance.put(attribute, provenance);
	}
	public Collection<String> getAttributeProvenance(String attribute) {
		return provenance.get(attribute);
	}
	public String getMergedAttributeProvenance(String attribute) {
		Collection<String> prov = provenance.get(attribute);
		
		if(prov!=null) {
			return StringUtils.join(prov, "+");
		} else {
			return "";
		}
	}
	
	public FusableBooks(String identifier, String provenance) {
		super(identifier, provenance);
	}

	@Override
	public Collection<String> getAttributeNames() {
		return Arrays.asList(new String[] { ISBN, BOOK_NAME, AUTHORS, PUBLISHER, GENRE, PAGES, PUBCOUNTRY, PUBYEAR, RATING });
	}

	@Override
	public boolean hasValue(String attributeName) {
		switch (attributeName) {
		case ISBN:
			return getISBN()!=null && !getISBN().isEmpty();
		case BOOK_NAME:
			return getBookName()!=null && !getBookName().isEmpty();
		case PUBLISHER:
			return getPublisher()!=null && !getPublisher().isEmpty();
		case AUTHORS:
			return getAuthors()!=null && getAuthors().size()>0;
		case GENRE:
			return getGenre()!=null && !getGenre().isEmpty();
		case PAGES:
			return getPages()!=null && !getPages().isEmpty();
		case PUBCOUNTRY:
			return getPubCounrty()!=null && !getPubCounrty().isEmpty();
		case PUBYEAR:
			return getYear()!=null && !getYear().isEmpty();
		case RATING:
			return getRating() != 0.0;
		default:
			return false;
		}
	}
	
	@Override
	public String toString() {
		return String.format("[Book: %s / %s / %s]", getISBN(), getBookName(), getPublisher(), getGenre(), getPages(), getPubCounrty(), getYear().toString());
	}

}
