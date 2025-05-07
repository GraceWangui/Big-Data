package bigdata.transformations.filters;

import bigdata.objects.AssetFeatures;
import bigdata.objects.AssetMetadata;
import org.apache.spark.api.java.JavaPairRDD;

/**
 * @author grace
 * This class
 *     Filters assets with Volatility score greater than or equal to 4 and
 *     Removes assets with a Price-to-Earnings Ratio greater than or equal to 25.
 *     Removes assets with a P/E ration of 0 or null
 */
public class FeatureFilter {
    /**
     Filters out assets with:
     P/E Ratio = 0
     P/E Ratio = null
     P/E Ratio ≥ 25
     */
    public static JavaPairRDD<String, AssetMetadata> filterByPERatio(JavaPairRDD<String, AssetMetadata> assetMetadata) {
        return assetMetadata.filter(entry -> {
            AssetMetadata metadata = entry._2;
            double peRatio = metadata.getPriceEarningRatio();
            return peRatio > 0 && peRatio < 25; // Keep only valid P/E Ratios
        });
    }

    /**
     Filter assets based on Volatility
     - Keeps only assets with Volatility < volatilityCeiling
     */
    public static JavaPairRDD<String, AssetFeatures> filterByVolatility(JavaPairRDD<String, AssetFeatures> assetFeaturesRDD, double volatilityCeiling) {
        return assetFeaturesRDD.filter(entry -> entry._2.getAssetVolitility() < volatilityCeiling);
    }
}
