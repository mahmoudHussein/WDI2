package de.uni_mannheim.informatik.wdi.datafusion.usecase.books.fusers;

import de.uni_mannheim.informatik.wdi.datafusion.AttributeValueFuser;
import de.uni_mannheim.informatik.wdi.datafusion.FusedValue;
import de.uni_mannheim.informatik.wdi.datafusion.RecordGroup;
import de.uni_mannheim.informatik.wdi.datafusion.conflictresolution.Voting;
import de.uni_mannheim.informatik.wdi.datafusion.conflictresolution.numeric.Average;
import de.uni_mannheim.informatik.wdi.datafusion.usecase.books.FusableBooks;

public class RatingFuser extends AttributeValueFuser<Double, FusableBooks>{

	public RatingFuser() {
		super(new Average<FusableBooks>());
	}

	public void fuse(RecordGroup<FusableBooks> group,
			FusableBooks fusedRecord) {
		
		// get the fused value
		FusedValue<Double, FusableBooks> fused = getFusedValue(group);
		
		// set the value for the fused record
		fusedRecord.setRating(fused.getValue());
		
		// add provenance info
		fusedRecord.setAttributeProvenance(FusableBooks.PUBYEAR, fused.getOriginalIds());
	}

	@Override
	public boolean hasValue(FusableBooks record) {
		return record.hasValue(FusableBooks.PUBYEAR);
	}
	
	@Override
	protected Double getValue(FusableBooks record) {
		return record.getRating();
	}

}
