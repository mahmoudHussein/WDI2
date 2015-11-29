package de.uni_mannheim.informatik.wdi.datafusion.usecase.books.fusers;

import de.uni_mannheim.informatik.wdi.datafusion.AttributeValueFuser;
import de.uni_mannheim.informatik.wdi.datafusion.FusedValue;
import de.uni_mannheim.informatik.wdi.datafusion.RecordGroup;
import de.uni_mannheim.informatik.wdi.datafusion.conflictresolution.string.LongestString;
import de.uni_mannheim.informatik.wdi.datafusion.usecase.books.FusableBooks;

public class ISBNFuser extends AttributeValueFuser<String, FusableBooks>{

	public ISBNFuser() {
		super(new LongestString<FusableBooks>());
	}

	public void fuse(RecordGroup<FusableBooks> group,
			FusableBooks fusedRecord) {
		
		// get the fused value
		FusedValue<String, FusableBooks> fused = getFusedValue(group);
		
		// set the value for the fused record
		fusedRecord.setISBN(fused.getValue());
		
		// add provenance info
		fusedRecord.setAttributeProvenance(FusableBooks.ISBN, fused.getOriginalIds());
	}

	@Override
	public boolean hasValue(FusableBooks record) {
		return record.hasValue(FusableBooks.ISBN);
	}
	
	@Override
	protected String getValue(FusableBooks record) {
		return record.getISBN();
	}

}
