package de.uni_mannheim.informatik.wdi.datafusion.usecase.books;

import org.w3c.dom.Document;
import org.w3c.dom.Element;

import de.uni_mannheim.informatik.wdi.datafusion.XMLFormatter;
import de.uni_mannheim.informatik.wdi.usecase.books.Authors;

public class AuthorsXMLFormatter extends XMLFormatter<Authors> {

	@Override
	public Element createRootElement(Document doc) {
		return doc.createElement("Authors");
	}

	@Override
	public Element createElementFromRecord(Authors record, Document doc) {
		Element author = doc.createElement("Author");
		
		author.appendChild(createTextElement("title", record.getAuthorName(), doc));
		
		return author;
	}

}
