import java.io.Closeable;
import java.io.IOException;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Implement the two methods below. We expect this class to be stateless and thread safe.
 */
public class Census {
    /**
     * Output format expected by our tests.
     */
    public static final String OUTPUT_FORMAT = "%d:%d=%d"; // Position:Age=Total
    /**
     * Number of cores in the current machine.
     */
    private static final int CORES = Runtime.getRuntime().availableProcessors();
    /**
     * Factory for iterators.
     */
    private final Function<String, AgeInputIterator> iteratorFactory;

    /**
     * Creates a new Census calculator.
     *
     * @param iteratorFactory factory for the iterators.
     */
    public Census(Function<String, AgeInputIterator> iteratorFactory) {
        this.iteratorFactory = iteratorFactory;
    }

    /**
     * The method iterates over the region
     * @param ageByRegionIterator
     * @param ageCountMap
     */
    private static void calculateCountOfEachAge(AgeInputIterator ageByRegionIterator, HashMap<Integer, Integer> ageCountMap) {
        while (ageByRegionIterator.hasNext()) {
            int age = ageByRegionIterator.next();
            if (age >= 0) {
                ageCountMap.put(age, ageCountMap.getOrDefault(age, 0) + 1);
            } else throw new IllegalArgumentException("Age is invalid");
        }
    }

    private static void getTop3Ages(HashMap<Integer, Integer> ageCountMap, List<String> top3Ages) {
        List<Map.Entry<Integer, Integer>> sortedByCounts = ageCountMap.entrySet().stream().sorted(Map.Entry.<Integer, Integer>comparingByValue().reversed()).collect(Collectors.toList());
        int prevValue = -1;
        int uniqueRankCount = 0;
        for (Map.Entry<Integer, Integer> entry : sortedByCounts) {
            Integer ageCount = entry.getValue();
            if (ageCount != prevValue) {
                uniqueRankCount++; // update rank only if the ageCount is unique
            }
            prevValue = ageCount;
            if (uniqueRankCount > 3) break;
            top3Ages.add(String.format(OUTPUT_FORMAT, uniqueRankCount, entry.getKey(), ageCount));
        }
    }

    /**
     * Given one region name, call {@link #iteratorFactory} to get an iterator for this region and return
     * the 3 most common ages in the format specified by {@link #OUTPUT_FORMAT}.
     */
    public String[] top3Ages(String region) {
        List<String> top3Ages = new ArrayList<>();
        HashMap<Integer, Integer> ageCountMap = new HashMap<>();
        try (AgeInputIterator ageByRegionIterator = iteratorFactory.apply(region)) {
            calculateCountOfEachAge(ageByRegionIterator, ageCountMap);
            getTop3Ages(ageCountMap, top3Ages);
            return top3Ages.toArray(new String[0]);
        } catch (IOException ioException) {
            throw new RuntimeException("Error occured while closing iterator");
        } catch (RuntimeException re) {
            if (re instanceof IllegalArgumentException)
                throw new RuntimeException(re.getMessage()); // used for age is invalid error
            return new String[]{}; // return empty in case of no region found

        }
    }

    /**
     * Given a list of region names, call {@link #iteratorFactory} to get an iterator for each region and return
     * the 3 most common ages across all regions in the format specified by {@link #OUTPUT_FORMAT}.
     * We expect you to make use of all cores in the machine, specified by {@link #CORES).
     */
    public String[] top3Ages(List<String> regionNames) {
        List<String> top3Ages = new ArrayList<>();
        HashMap<Integer, Integer> ageCountMap = new HashMap<>();
        for (String region : regionNames) {
            try (AgeInputIterator ageByRegionIterator = iteratorFactory.apply(region)) {
                calculateCountOfEachAge(ageByRegionIterator, ageCountMap); // accumlate all age counts by region into one map
            } catch (IOException ioException) {
                throw new RuntimeException("Error occurred while closing iterator");
            } catch (RuntimeException re) {
                if (re instanceof IllegalArgumentException)
                    throw new RuntimeException(re.getMessage()); // used for age is invalid error
            }
        }
        getTop3Ages(ageCountMap, top3Ages);
        return top3Ages.toArray(new String[0]);
    }


    /**
     * Implementations of this interface will return ages on call to {@link Iterator#next()}. They may open resources
     * when being instantiated created.
     */
    public interface AgeInputIterator extends Iterator<Integer>, Closeable {
    }
}
