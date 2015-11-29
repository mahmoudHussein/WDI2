package de.uni_mannheim.informatik.wdi.datafusion.usecase.books.fusers;

import de.uni_mannheim.informatik.wdi.datafusion.AttributeValueFuser;
import de.uni_mannheim.informatik.wdi.datafusion.FusedValue;
import de.uni_mannheim.informatik.wdi.datafusion.RecordGroup;
import de.uni_mannheim.informatik.wdi.datafusion.conflictresolution.ConflictResolutionFunction;
import de.uni_mannheim.informatik.wdi.datafusion.conflictresolution.string.LongestString;
import de.uni_mannheim.informatik.wdi.datafusion.usecase.books.FusableBooks;
import de.uni_mannheim.informatik.wdi.datafusion.usecase.movies.FusableMovie;


public class BookTitleFuser extends AttributeValueFuser<String, FusableBooks>{

	public BookTitleFuser() {
		super(new LongestString<FusableBooks>());
	}

	public void fuse(RecordGroup<FusableBooks> group,
			FusableBooks fusedRecord) {
		
		// get the fused value
		FusedValue<String, FusableBooks> fused = getFusedValue(group);
		
		// set the value for the fused record
		fusedRecord.setBookName(fused.getValue());
		
		// add provenance info
		fusedRecord.setAttributeProvenance(FusableBooks.BOOK_NAME, fused.getOriginalIds());
	}

	@Override
	public boolean hasValue(FusableBooks record) {
		return record.hasValue(FusableBooks.BOOK_NAME);
	}
	
	@Override
	protected String getValue(FusableBooks record) {
		return record.getBookName();
	}
}
