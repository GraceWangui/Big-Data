package bigdata.transformations.filters;

import bigdata.objects.StockPrice;
import bigdata.util.TimeUtil;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Dataset;

import java.time.Instant;



/**
 * @author grace
 * This class filters the data such that we work with data
 * from the prior trading year from the get-go
 */
public class DatesFilter {
    public static Dataset<StockPrice> filterStockPrices(Dataset<StockPrice> prices, String datasetEndDate) {
        // Compute start date (251 trading days before datasetEndDate)
        Instant endDate = TimeUtil.fromDate(datasetEndDate);
        //I use 356 days instead of 251 to get the correct start date as the 251 does not include weekends
        long tradingDaysInSeconds = 356L * 24 * 60 * 60;
        Instant startDate = endDate.minusSeconds(tradingDaysInSeconds);

        // Filter stock prices to keep only those from the last 251 trading days
        Dataset<StockPrice> filteredPrices = prices.filter((FilterFunction<StockPrice>) stock -> {
            Instant stockDate = TimeUtil.fromDate(stock.getYear(), stock.getMonth(), stock.getDay());
            return !stockDate.isBefore(startDate) && !stockDate.isAfter(endDate);
        });


        return filteredPrices;
    }
}
