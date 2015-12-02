package de.uni_mannheim.informatik.wdi.datafusion.usecase.books;



import java.io.File;
import java.io.IOException;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.xpath.XPathExpressionException;

import org.joda.time.DateTime;
import org.xml.sax.SAXException;

import de.uni_mannheim.informatik.wdi.DataSet;
import de.uni_mannheim.informatik.wdi.datafusion.CorrespondenceSet;
import de.uni_mannheim.informatik.wdi.datafusion.DataFusionEngine;
import de.uni_mannheim.informatik.wdi.datafusion.DataFusionStrategy;
import de.uni_mannheim.informatik.wdi.datafusion.FusableDataSet;
import de.uni_mannheim.informatik.wdi.datafusion.evaluation.DataFusionEvaluator;
import de.uni_mannheim.informatik.wdi.datafusion.usecase.books.evaluation.AuthorsEvaluationRule;
import de.uni_mannheim.informatik.wdi.datafusion.usecase.books.evaluation.BookTitleEvaluationRule;
import de.uni_mannheim.informatik.wdi.datafusion.usecase.books.evaluation.GenreEvaluationRule;
import de.uni_mannheim.informatik.wdi.datafusion.usecase.books.evaluation.ISBNEvaluationRule;
import de.uni_mannheim.informatik.wdi.datafusion.usecase.books.evaluation.PagesEvaluationRule;
import de.uni_mannheim.informatik.wdi.datafusion.usecase.books.evaluation.PubCountryEvaluationRule;
import de.uni_mannheim.informatik.wdi.datafusion.usecase.books.evaluation.PubYearEvaluationRule;
import de.uni_mannheim.informatik.wdi.datafusion.usecase.books.evaluation.PublisherEvaluationRule;
import de.uni_mannheim.informatik.wdi.datafusion.usecase.books.evaluation.RatingEvaluationRule;
import de.uni_mannheim.informatik.wdi.datafusion.usecase.books.fusers.AuthorFuser;
import de.uni_mannheim.informatik.wdi.datafusion.usecase.books.fusers.BookTitleFuser;
import de.uni_mannheim.informatik.wdi.datafusion.usecase.books.fusers.GenreFuser;
import de.uni_mannheim.informatik.wdi.datafusion.usecase.books.fusers.ISBNFuser;
import de.uni_mannheim.informatik.wdi.datafusion.usecase.books.fusers.PagesFuser;
import de.uni_mannheim.informatik.wdi.datafusion.usecase.books.fusers.PubCountryFuser;
import de.uni_mannheim.informatik.wdi.datafusion.usecase.books.fusers.PubDateFuser;
import de.uni_mannheim.informatik.wdi.datafusion.usecase.books.fusers.PublisherFuser;
import de.uni_mannheim.informatik.wdi.datafusion.usecase.books.fusers.RatingFuser;
import de.uni_mannheim.informatik.wdi.datafusion.usecase.movies.FusableMovie;
import de.uni_mannheim.informatik.wdi.datafusion.usecase.movies.evaluation.ActorsEvaluationRule;
import de.uni_mannheim.informatik.wdi.datafusion.usecase.movies.evaluation.DateEvaluationRule;
import de.uni_mannheim.informatik.wdi.datafusion.usecase.movies.evaluation.DirectorEvaluationRule;
import de.uni_mannheim.informatik.wdi.datafusion.usecase.movies.evaluation.TitleEvaluationRule;
import de.uni_mannheim.informatik.wdi.datafusion.usecase.movies.fusers.ActorsFuser;
import de.uni_mannheim.informatik.wdi.datafusion.usecase.movies.fusers.DateFuser;
import de.uni_mannheim.informatik.wdi.datafusion.usecase.movies.fusers.DirectorFuser;
import de.uni_mannheim.informatik.wdi.datafusion.usecase.movies.fusers.TitleFuser;

public class Books_Main {

	public static void main(String[] args) throws XPathExpressionException, ParserConfigurationException, SAXException, IOException, TransformerException {
		// load the data sets
		FusableDataSet<FusableBooks> ds1 = new FusableDataSet<>();
		FusableDataSet<FusableBooks> ds2 = new FusableDataSet<>();
		FusableDataSet<FusableBooks> ds3 = new FusableDataSet<>();
		FusableDataSet<FusableBooks> ds4 = new FusableDataSet<>();
		ds1.loadFromXML(
				new File("usecase/books/input/AuthorTargetSchemaB.xml"),
				new FusableBooksFactory(), "/Books/Book");
		ds2.loadFromXML(
				new File("usecase/books/input/DBPediaTargetSchemaBooks.xml"),
				new FusableBooksFactory(), "/Books/Book");
		ds3.loadFromXML(
				new File("usecase/books/input/GoodReadsTargetSchema.xml"), 
				new FusableBooksFactory(), 
				"/Books/Book");
//		ds3.loadFromXML(
//				new File("usecase/books/input/FreiburgTargetSchemaOutput.xml"), 
//				new FusableBooksFactory(), 
//				"/Books/Book");
		
		// set dataset metadata
//		ds1.setScore(4.0);
//		ds2.setScore(3.0);
//		ds3.setScore(1.0);
//		ds4.setScore(2.0);
		
		ds1.setScore(3.0);
		ds2.setScore(2.0);
		ds3.setScore(1.0);
		
		
		ds1.setDate(DateTime.parse("2015-11-12"));
		ds2.setDate(DateTime.parse("2015-11-12"));
		ds3.setDate(DateTime.parse("2015-10-15"));
//		ds4.setDate(DateTime.parse("2014-09-01"));
		// print dataset density
		System.out.println("AuthorTargetSchemaB.xml");
		ds1.printDataSetDensityReport();
		System.out.println("DBPediaTargetSchemaBooks.xml");
		ds2.printDataSetDensityReport();
		System.out.println("GoodReadsTargetSchema.xml");
		ds3.printDataSetDensityReport();
//		System.out.println("FreiburgTargetSchemaOutput.xml");
//		ds4.printDataSetDensityReport();
		
		// load the correspondences
		CorrespondenceSet<FusableBooks> correspondences = new CorrespondenceSet<>();
		correspondences.loadCorrespondences(new File("usecase/books/correspondences/Author_2_DbpediaBooks_Correspondences.csv"), ds1, ds2);
		correspondences.loadCorrespondences(new File("usecase/books/correspondences/Author_2_GoodReads_Correspondences.csv"), ds1, ds3);
		correspondences.loadCorrespondences(new File("usecase/books/correspondences/GoodReads_2_DbpediaBooks_Correspondences.csv"), ds3, ds2);
		
		// write group size distribution
		correspondences.writeGroupSizeDistribution(new File("usecase/Books/output/group_size_distribution.csv"));
		
		// define the fusion strategy
		DataFusionStrategy<FusableBooks> strategy = new DataFusionStrategy<>(new FusableBooksFactory());
		// add attribute fusers
		// Note: The attribute name is only used for printing the reports
		strategy.addAttributeFuser("ISBN", new ISBNFuser(), new ISBNEvaluationRule());
		strategy.addAttributeFuser("Book_Name", new BookTitleFuser(), new BookTitleEvaluationRule());
		strategy.addAttributeFuser("Authors", new AuthorFuser() , new AuthorsEvaluationRule());
		strategy.addAttributeFuser("Publisher", new PublisherFuser(), new PublisherEvaluationRule());
//		strategy.addAttributeFuser("Genre", new GenreFuser(), new GenreEvaluationRule());
//		strategy.addAttributeFuser("Pages", new PagesFuser(), new PagesEvaluationRule());
//		strategy.addAttributeFuser("Publication_Country", new PubCountryFuser(), new PubCountryEvaluationRule());
		strategy.addAttributeFuser("Publication_Date", new PubDateFuser(), new PubYearEvaluationRule());
		strategy.addAttributeFuser("Rating", new RatingFuser(), new RatingEvaluationRule());
		// create the fusion engine
		DataFusionEngine<FusableBooks> engine = new DataFusionEngine<>(strategy);
		
		// calculate cluster consistency
		engine.printClusterConsistencyReport(correspondences);
		
		// run the fusion
		FusableDataSet<FusableBooks> fusedDataSet = engine.run(correspondences);
		
		// write the result
		fusedDataSet.writeXML(new File("usecase/book/output/fused.xml"), new BookXMLFormatter());
		
		// load the gold standard
		DataSet<FusableBooks> gs = new FusableDataSet<>();
		gs.loadFromXML(
				new File("usecase/Books/goldstandard/fused.xml"),
				new FusableBooksFactory(), "/Books/Books");
		
		// evaluate
		DataFusionEvaluator<FusableBooks> evaluator = new DataFusionEvaluator<>(strategy);
		evaluator.setVerbose(true);
		double accuracy = evaluator.evaluate(fusedDataSet, gs);
		
		System.out.println(String.format("Accuracy: %.2f", accuracy));
		
	}
	
}
