import java.io.Closeable;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
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
     * The method iterates over the region and helps keeps count per age
     * @param ageByRegionIterator
     * @param ageCountMap
     */
    private static void calculateCountOfEachAge(AgeInputIterator ageByRegionIterator, ConcurrentMap<Integer, Integer> ageCountMap) {
        while (ageByRegionIterator.hasNext()) {
            int age = ageByRegionIterator.next();
            if (age >= 0) {
                ageCountMap.merge(age, 1, Integer::sum); // need to use merge and sum function as it is thread safe and prevents the race condition
            } else throw new IllegalArgumentException("Age is invalid");
        }
    }

    /**
     * Sort
     * @param ageCountMap
     * @param top3Ages
     */
    private static void getTop3Ages(ConcurrentMap<Integer, Integer> ageCountMap, List<String> top3Ages) {
        // Min-Heap (PriorityQueue) to track top age-count pairs
        PriorityQueue<Map.Entry<Integer, Integer>> minHeap = new PriorityQueue<>(
                Comparator.comparingInt((Map.Entry<Integer, Integer> e) -> e.getValue()).reversed() // Sort by count descending
                        .thenComparing(Map.Entry::getKey) // Sort by age ascending for ties

        );

        // Find the top 3 unique count groups
        for (Map.Entry<Integer, Integer> entry : ageCountMap.entrySet()) {
            minHeap.offer(entry);

            // Check if we are exceeding size 3
            if (minHeap.size() > 3) {
                // Peek the lowest count in heap

                // Remove only if this value is lower than the current entry’s value
                while (!minHeap.isEmpty() && minHeap.peek().getValue() < entry.getValue()) {
                    minHeap.poll();
                }
            }
        }
        System.out.println(minHeap.size());

        // Convert heap to list and sort it in descending order (highest count first)
        List<Map.Entry<Integer, Integer>> topEntries = new ArrayList<>(minHeap);
        topEntries.sort(
                Comparator.comparingInt((Map.Entry<Integer, Integer> e) -> e.getValue()).reversed() // Sort by count descending
                        .thenComparing(Map.Entry::getKey) // Sort by age ascending for ties
        );

        // Ranking logic
        int prevCount = -1;
        int uniqueRankCount = 0;

        for (Map.Entry<Integer, Integer> entry : topEntries) {
            if (entry.getValue() != prevCount) {
                uniqueRankCount++; // Update rank only for unique values
            }
            prevCount = entry.getValue();
            if(uniqueRankCount >3) break;

            // Format output as "Rank:Age=Count"
            top3Ages.add(String.format(OUTPUT_FORMAT, uniqueRankCount, entry.getKey(), entry.getValue()));
        }
    }

    /**
     * Given one region name, call {@link #iteratorFactory} to get an iterator for this region and return
     * the 3 most common ages in the format specified by {@link #OUTPUT_FORMAT}.
     */
    public String[] top3Ages(String region) {
        List<String> top3Ages = new ArrayList<>();
        ConcurrentHashMap<Integer, Integer> ageCountMap = new ConcurrentHashMap<>();
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
        List<String> resultList = new ArrayList<>();
        ConcurrentMap<Integer, Integer> ageCountMap = new ConcurrentHashMap<>();
        List<CompletableFuture<Void>> futures = new ArrayList<>();

        ExecutorService executor = Executors.newFixedThreadPool(CORES);
        try {
            for (String region : regionNames) {
                CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                    try (AgeInputIterator iterator = iteratorFactory.apply(region)) {
                        calculateCountOfEachAge(iterator, ageCountMap);
                    } catch (IOException e) {
                        // Log and ignore; iterator resource issues should not crash the entire process.
                        System.err.println("IOException in region " + region + ": " + e.getMessage());
                    } catch (IllegalArgumentException e) {
                        // For invalid ages, you might choose to propagate the exception or log it.
                        System.err.println("Invalid age in region " + region + ": " + e.getMessage());
                        // Optionally rethrow if you want to fail the whole operation:
                        // throw new RuntimeException(e.getMessage(), e);
                    } catch (RuntimeException e) {
                        // Log unexpected runtime exceptions
                        System.err.println("Unexpected error in region " + region + ": " + e.getMessage());
                    }
                }, executor);
                futures.add(future);
            }
            // Wait for all tasks to complete.
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        } finally {
            executor.shutdown();
            // Optionally await termination.
        }
        getTop3Ages(ageCountMap, resultList);
        return resultList.toArray(new String[0]);
    }



    /**
     * Implementations of this interface will return ages on call to {@link Iterator#next()}. They may open resources
     * when being instantiated created.
     */
    public interface AgeInputIterator extends Iterator<Integer>, Closeable {
    }
}
