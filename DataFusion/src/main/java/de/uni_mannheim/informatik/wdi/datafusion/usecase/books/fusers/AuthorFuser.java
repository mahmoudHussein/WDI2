package de.uni_mannheim.informatik.wdi.datafusion.usecase.books.fusers;

import java.util.List;

import de.uni_mannheim.informatik.wdi.datafusion.AttributeValueFuser;
import de.uni_mannheim.informatik.wdi.datafusion.FusedValue;
import de.uni_mannheim.informatik.wdi.datafusion.RecordGroup;
import de.uni_mannheim.informatik.wdi.datafusion.conflictresolution.list.Union;
import de.uni_mannheim.informatik.wdi.datafusion.usecase.books.FusableBooks;
import de.uni_mannheim.informatik.wdi.usecase.books.Authors;

public class AuthorFuser extends AttributeValueFuser<List<Authors>, FusableBooks> {

	public AuthorFuser() {
		super(new Union<Authors, FusableBooks>());
	}

	@Override
	protected List<Authors> getValue(FusableBooks record) {
		return record.getAuthors();
	}

	@Override
	public void fuse(RecordGroup<FusableBooks> group, FusableBooks fusedRecord) {
		FusedValue<List<Authors>, FusableBooks> fused = getFusedValue(group);
		fusedRecord.setAuthor(fused.getValue());
		fusedRecord.setAttributeProvenance(FusableBooks.AUTHORS, fused.getOriginalIds());
	}

	@Override
	public boolean hasValue(FusableBooks record) {
		return record.hasValue(FusableBooks.AUTHORS);
	}

}
