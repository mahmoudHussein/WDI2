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
		ds3.loadFromXML(
				new File("usecase/books/input/FreiburgTargetSchemaOutput.xml"), 
				new FusableBooksFactory(), 
				"/Books/Book");
		
		// set dataset metadata
		ds1.setScore(4.0);
		ds2.setScore(3.0);
		ds3.setScore(1.0);
		ds4.setScore(2.0);
		
		ds1.setDate(DateTime.parse("2012-01-01"));
		ds2.setDate(DateTime.parse("2010-01-01"));
		ds3.setDate(DateTime.parse("2008-01-01"));
		
		// print dataset density
		System.out.println("AuthorTargetSchemaB.xml");
		ds1.printDataSetDensityReport();
		System.out.println("DBPediaTargetSchemaBooks.xml");
		ds2.printDataSetDensityReport();
		System.out.println("GoodReadsTargetSchema.xml");
		ds3.printDataSetDensityReport();
		System.out.println("FreiburgTargetSchemaOutput.xml");
		ds4.printDataSetDensityReport();
		
		// load the correspondences
		CorrespondenceSet<FusableBooks> correspondences = new CorrespondenceSet<>();
		correspondences.loadCorrespondences(new File("usecase/books/correspondences/Author_2_DbpediaBooks_Correspondences.csv"), ds1, ds2);
		correspondences.loadCorrespondences(new File("usecase/books/correspondences/Author_2_GoodReads_Correspondences.csv"), ds1, ds3);
		correspondences.loadCorrespondences(new File("usecase/books/correspondences/GoodReads_2_DbpediaBooks_Correspondences.csv"), ds3, ds2);
		
		// write group size distribution
		correspondences.writeGroupSizeDistribution(new File("usecase/movie/output/group_size_distribution.csv"));
		
		// define the fusion strategy
		DataFusionStrategy<FusableMovie> strategy = new DataFusionStrategy<>(new FusableBooksFactory());
		// add attribute fusers
		// Note: The attribute name is only used for printing the reports
		strategy.addAttributeFuser("Title", new TitleFuser(), new TitleEvaluationRule());
		strategy.addAttributeFuser("Director", new DirectorFuser(), new DirectorEvaluationRule());
		strategy.addAttributeFuser("Date", new DateFuser(), new DateEvaluationRule());
		strategy.addAttributeFuser("Actors", new ActorsFuser(), new ActorsEvaluationRule());
		
		// create the fusion engine
		DataFusionEngine<FusableMovie> engine = new DataFusionEngine<>(strategy);
		
		// calculate cluster consistency
		engine.printClusterConsistencyReport(correspondences);
		
		// run the fusion
		FusableDataSet<FusableMovie> fusedDataSet = engine.run(correspondences);
		
		// write the result
		fusedDataSet.writeXML(new File("usecase/movie/output/fused.xml"), new MovieXMLFormatter());
		
		// load the gold standard
		DataSet<FusableMovie> gs = new FusableDataSet<>();
		gs.loadFromXML(
				new File("usecase/movie/goldstandard/fused.xml"),
				new FusableMovieFactory(), "/movies/movie");
		
		// evaluate
		DataFusionEvaluator<FusableMovie> evaluator = new DataFusionEvaluator<>(strategy);
		evaluator.setVerbose(true);
		double accuracy = evaluator.evaluate(fusedDataSet, gs);
		
		System.out.println(String.format("Accuracy: %.2f", accuracy));
		
	}
	
}
