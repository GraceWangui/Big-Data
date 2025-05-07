package bigdata.app;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

import bigdata.objects.*;
import bigdata.technicalindicators.Returns;
import bigdata.technicalindicators.Volitility;
import bigdata.transformations.filters.DatesFilter;
import bigdata.transformations.filters.FeatureFilter;
import bigdata.util.TimeUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.broadcast.Broadcast;

import bigdata.transformations.filters.NullPriceFilter;
import bigdata.transformations.maps.PriceReaderMap;
import bigdata.transformations.pairing.AssetMetadataPairing;
import scala.Tuple2;

import static org.apache.spark.sql.functions.col;

public class AssessedExercise {

	public static void main(String[] args) throws InterruptedException {

		//--------------------------------------------------------
	    // Static Configuration
	    //--------------------------------------------------------
		String datasetEndDate = "2020-04-01";
		double volatilityCeiling = 4;
		double peRatioThreshold = 25;

		long startTime = System.currentTimeMillis();

		// The code submitted for the assessed exerise may be run in either local or remote modes
		// Configuration of this will be performed based on an environment variable
		String sparkMasterDef = System.getenv("SPARK_MASTER");
		if (sparkMasterDef==null) {
			File hadoopDIR = new File("resources/hadoop/"); // represent the hadoop directory as a Java file so we can get an absolute path for it
			System.setProperty("hadoop.home.dir", hadoopDIR.getAbsolutePath()); // set the JVM system property so that Spark finds it
			sparkMasterDef = "local[4]"; // default is local mode with two executors
		}

		String sparkSessionName = "BigDataAE"; // give the session a name

		// Create the Spark Configuration
		SparkConf conf = new SparkConf()
				.setMaster(sparkMasterDef)
				.setAppName(sparkSessionName);

		// Create the spark session
		SparkSession spark = SparkSession
				  .builder()
				  .config(conf)
				  .getOrCreate();


		// Get the location of the asset pricing data
		String pricesFile = System.getenv("BIGDATA_PRICES");
		if (pricesFile==null) pricesFile = "resources/all_prices-noHead.csv"; // default is a sample with 3 queries

		// Get the asset metadata
		String assetsFile = System.getenv("BIGDATA_ASSETS");
		if (assetsFile==null) assetsFile = "resources/stock_data.json"; // default is a sample with 3 queries


    	//----------------------------------------
    	// Pre-provided code for loading the data
    	//----------------------------------------

    	// Create Datasets based on the input files

		// Load in the assets, this is a relatively small file
		Dataset<Row> assetRows = spark.read().option("multiLine", true).json(assetsFile);
		//assetRows.printSchema();
		System.err.println(assetRows.first().toString());
		JavaPairRDD<String, AssetMetadata> assetMetadata = assetRows.toJavaRDD().mapToPair(new AssetMetadataPairing());

		// Load in the prices, this is a large file (not so much in data size, but in number of records)
    	Dataset<Row> priceRows = spark.read().csv(pricesFile); // read CSV file
    	Dataset<Row> priceRowsNoNull = priceRows.filter(new NullPriceFilter()); // filter out rows with null prices
    	Dataset<StockPrice> prices = priceRowsNoNull.map(new PriceReaderMap(), Encoders.bean(StockPrice.class)); // Convert to Stock Price Objects


		AssetRanking finalRanking = rankInvestments(spark, assetMetadata, prices, datasetEndDate, volatilityCeiling, peRatioThreshold);

		System.out.println(finalRanking.toString());


		System.out.println("Holding Spark UI open for 1 minute: http://localhost:4040");

		Thread.sleep(60000);


		// Close the spark session
		spark.close();

		String out = System.getenv("BIGDATA_RESULTS");
		String resultsDIR = "results/";
		if (out!=null) resultsDIR = out;



		long endTime = System.currentTimeMillis();

		try {
			BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(new File(resultsDIR).getAbsolutePath()+"/SPARK.DONE")));

			Instant sinstant = Instant.ofEpochSecond( startTime/1000 );
			Date sdate = Date.from( sinstant );

			Instant einstant = Instant.ofEpochSecond( endTime/1000 );
			Date edate = Date.from( einstant );

			writer.write("StartTime:"+sdate.toGMTString()+'\n');
			writer.write("EndTime:"+edate.toGMTString()+'\n');
			writer.write("Seconds: "+((endTime-startTime)/1000)+'\n');
			writer.write('\n');
			writer.write(finalRanking.toString());
			writer.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}


	public static AssetRanking rankInvestments(SparkSession spark, JavaPairRDD<String, AssetMetadata> assetMetadata, Dataset<StockPrice> prices, String datasetEndDate, double volatilityCeiling, double peRatioThreshold) {

    	//----------------------------------------
    	// Student's solution starts here
    	//----------------------------------------

		//For the first step, I filter the data to work only with data from the prior year, which is 251 trading days
		//This is implemented in the DatesFilter class
		// Filter stock prices by date using DatesFilter class
		Dataset<StockPrice> filteredPrices = DatesFilter.filterStockPrices(prices, datasetEndDate).cache();


		//Filter asset metadata by valid P/E Ratio
		//  Filters out assets with:P/E Ratio = 0, P/E Ratio = null, P/E Ratio ≥ 25
		JavaPairRDD<String, AssetMetadata> filteredMetadata = FeatureFilter.filterByPERatio(assetMetadata);

		// Convert filteredPrices into a PairRDD (Symbol Ticker, StockPrice)
		JavaPairRDD<String, StockPrice> tickerPricesRDD = filteredPrices.javaRDD()
				.mapToPair((PairFunction<StockPrice, String, StockPrice>) stock ->
						new Tuple2<>(stock.getStockTicker(), stock));

		 //Group by Ticker (Key) using reduceByKey instead of groupByKey
		JavaPairRDD<String, List<StockPrice>> groupedPrices = tickerPricesRDD
				.mapValues(stock -> Collections.singletonList(stock))
				.reduceByKey((list1, list2) -> {
					List<StockPrice> merged = new ArrayList<>(list1);
					merged.addAll(list2);
					return merged;
				});

		// Sort each group by date (Latest First) & Extract Closing Prices
		//Closing prices will then be used to compute volatility and returns hence why the extraction of them
		JavaPairRDD<String, List<Double>> closingPricesRDD = groupedPrices.mapValues(pricesList ->
				pricesList.stream()
						.sorted(Comparator.comparing(s -> TimeUtil.fromDate(s.getYear(), s.getMonth(), s.getDay())))
						.map(price -> Double.valueOf(price.getClosePrice())) //
						.collect(Collectors.toList())
		).cache();


		// Broadcast metadata for efficient joining
		Broadcast<Map<String, AssetMetadata>> metadataBroadcast =
				new JavaSparkContext(spark.sparkContext()).broadcast(filteredMetadata.collectAsMap());


		// Perform map-side join instead of shuffle join to make computation effective and Handle missing metadata
		JavaPairRDD<String, Tuple2<List<Double>, AssetMetadata>> joinedData = closingPricesRDD
				.flatMapToPair(entry -> {
					AssetMetadata metadata = metadataBroadcast.value().get(entry._1);
					if (metadata == null) return Collections.emptyIterator(); //Skip missing metadata
					return Collections.singleton(new Tuple2<>(entry._1, new Tuple2<>(entry._2, metadata))).iterator();
				});


		//Compute Volatility, Returns, and Set P/E Ratio
		JavaPairRDD<String, AssetFeatures> assetFeaturesRDD = joinedData.mapValues(data -> {
			List<Double> closingPrices = data._1; //closing prices is the first index of the joined data
			AssetMetadata metadata = data._2; //asset metadata is at the second index of the joined data

			// Compute Volatility & Returns
            // Make a call to the Volitility class to calculate volitility
			double volatility = Volitility.calculate(closingPrices);
			// Make a call to the Returns class to calculate returns
			double returns = Returns.calculate(5, closingPrices);

			// Create AssetFeatures with P/E Ratio from metadata and computed volatility and returns
			return new AssetFeatures(volatility, returns, metadata.getPriceEarningRatio());

		});


		// Apply feature filtering using FeatureFilter class to filter volatility
		// Filter for  Volatility score greater than or equal to 4 and

		JavaPairRDD<String, AssetFeatures> filteredAssets = FeatureFilter.filterByVolatility(
				assetFeaturesRDD, volatilityCeiling);


		//Sort by the computed Returns On Investments in descending order
		List<Tuple2<String, AssetFeatures>> sortedAssets = filteredAssets
				.mapToPair(entry -> new Tuple2<>(entry._2.getAssetReturn(), entry)) // Sort by ROI
				.sortByKey(false) // Descending order (highest ROI first)
				.values()
				.take(5); // Select Top 5

		// Convert to Asset objects
		List<Asset> topAssets = sortedAssets.stream().map(entry -> {
			String ticker = entry._1;
			AssetFeatures features = entry._2;

			// Get metadata
			AssetMetadata metadata = metadataBroadcast.value().get(ticker); // Lookup the metadata

			// Create an Asset object
			return new Asset(ticker, features, metadata.getName(), metadata.getIndustry(), metadata.getSector());
		}).collect(Collectors.toList());


		AssetRanking finalRanking = new AssetRanking(topAssets.toArray(new Asset[5])); // ...One of these is what your Spark program should collect


    	return finalRanking;


    }


}
