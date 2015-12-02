package de.uni_mannheim.informatik.wdi.datafusion.usecase.books.fusers;

import de.uni_mannheim.informatik.wdi.datafusion.AttributeValueFuser;
import de.uni_mannheim.informatik.wdi.datafusion.FusedValue;
import de.uni_mannheim.informatik.wdi.datafusion.RecordGroup;
import de.uni_mannheim.informatik.wdi.datafusion.conflictresolution.Voting;
import de.uni_mannheim.informatik.wdi.datafusion.usecase.books.FusableBooks;

public class PagesFuser extends AttributeValueFuser<String, FusableBooks>{

	public PagesFuser() {
		super(new Voting<String, FusableBooks>());
	}

	public void fuse(RecordGroup<FusableBooks> group,
			FusableBooks fusedRecord) {
		
		// get the fused value
		FusedValue<String, FusableBooks> fused = getFusedValue(group);
		
		// set the value for the fused record
		fusedRecord.setPages(fused.getValue());
		
		// add provenance info
		fusedRecord.setAttributeProvenance(FusableBooks.PAGES, fused.getOriginalIds());
	}

	@Override
	public boolean hasValue(FusableBooks record) {
		return record.hasValue(FusableBooks.PAGES);
	}
	
	@Override
	protected String getValue(FusableBooks record) {
		return record.getPages();
	}

}