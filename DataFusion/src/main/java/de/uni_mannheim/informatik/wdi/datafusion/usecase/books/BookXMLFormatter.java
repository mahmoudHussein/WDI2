package de.uni_mannheim.informatik.wdi.datafusion.usecase.books;

import org.w3c.dom.Document;
import org.w3c.dom.Element;

import de.uni_mannheim.informatik.wdi.datafusion.XMLFormatter;
import de.uni_mannheim.informatik.wdi.usecase.books.Authors;

public class BookXMLFormatter extends XMLFormatter<FusableBooks> {

	AuthorsXMLFormatter AuthorFormatter = new AuthorsXMLFormatter();
	
	@Override
	public Element createRootElement(Document doc) {
		return doc.createElement("Books");
	}

	@Override
	public Element createElementFromRecord(FusableBooks record, Document doc) {
		Element Book = doc.createElement("Book");

		Book.appendChild(createTextElement("Id", record.getIdentifier(), doc));
		
		Book.appendChild(createTextElementWithProvenance("ISBN", record.getISBN(), record.getMergedAttributeProvenance(FusableBooks.ISBN),doc));
		Book.appendChild(createTextElementWithProvenance("Book_Name", record.getBookName(), record.getMergedAttributeProvenance(FusableBooks.BOOK_NAME),doc));
		Book.appendChild(createTextElementWithProvenance("Publication_Date", record.getYear(), record.getMergedAttributeProvenance(FusableBooks.PUBYEAR),doc));
		Book.appendChild(createTextElementWithProvenance("Publisher", record.getPublisher(), record.getMergedAttributeProvenance(FusableBooks.PUBLISHER),doc));
		Book.appendChild(createTextElementWithProvenance("Publication_Country", record.getPubCounrty(), record.getMergedAttributeProvenance(FusableBooks.PUBCOUNTRY),doc));
		Book.appendChild(createTextElementWithProvenance("Pages", record.getPages(), record.getMergedAttributeProvenance(FusableBooks.PAGES),doc));
		Book.appendChild(createTextElementWithProvenance("Genre", record.getGenre(), record.getMergedAttributeProvenance(FusableBooks.GENRE),doc));
		Book.appendChild(createTextElementWithProvenance("Rating", record.getRating()+"", record.getMergedAttributeProvenance(FusableBooks.RATING),doc));
		Book.appendChild(createActorsElement(record, doc));
		
		return Book;
	}

	protected Element createTextElementWithProvenance(String name, String value, String provenance, Document doc) {
		Element elem = createTextElement(name, value, doc);
		elem.setAttribute("provenance", provenance);
		return elem;
	}
	
	protected Element createActorsElement(FusableBooks record, Document doc) {
		Element actorRoot = AuthorFormatter.createRootElement(doc);
		actorRoot.setAttribute("provenanec", record.getMergedAttributeProvenance(FusableBooks.AUTHORS));
		
		for(Authors a : record.getAuthors()) {
			actorRoot.appendChild(AuthorFormatter.createElementFromRecord(a, doc));
		}
		
		return actorRoot;
	}
	
}
