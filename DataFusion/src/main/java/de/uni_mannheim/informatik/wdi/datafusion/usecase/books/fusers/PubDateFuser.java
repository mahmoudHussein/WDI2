package de.uni_mannheim.informatik.wdi.datafusion.usecase.books.fusers;

import de.uni_mannheim.informatik.wdi.datafusion.AttributeValueFuser;
import de.uni_mannheim.informatik.wdi.datafusion.FusedValue;
import de.uni_mannheim.informatik.wdi.datafusion.RecordGroup;
import de.uni_mannheim.informatik.wdi.datafusion.conflictresolution.Voting;
import de.uni_mannheim.informatik.wdi.datafusion.usecase.books.FusableBooks;

public class PubDateFuser extends AttributeValueFuser<String, FusableBooks>{

	public PubDateFuser() {
		super(new Voting<String, FusableBooks>());
	}

	public void fuse(RecordGroup<FusableBooks> group,
			FusableBooks fusedRecord) {
		
		// get the fused value
		FusedValue<String, FusableBooks> fused = getFusedValue(group);
		
		// set the value for the fused record
		fusedRecord.setYear(fused.getValue());
		
		// add provenance info
		fusedRecord.setAttributeProvenance(FusableBooks.PUBYEAR, fused.getOriginalIds());
	}

	@Override
	public boolean hasValue(FusableBooks record) {
		return record.hasValue(FusableBooks.PUBYEAR);
	}
	
	@Override
	protected String getValue(FusableBooks record) {
		return record.getYear();
	}

}
