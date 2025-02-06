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
     * Number of cores in the current machine.
     */
    private static final int CORES = Runtime.getRuntime().availableProcessors();

    /**
     * Output format expected by our tests.
     */
    public static final String OUTPUT_FORMAT = "%d:%d=%d"; // Position:Age=Total

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
     * Given one region name, call {@link #iteratorFactory} to get an iterator for this region and return
     * the 3 most common ages in the format specified by {@link #OUTPUT_FORMAT}.
     */
    public String[] top3Ages(String region) {
        List<String> top3Ages = new ArrayList<>();
        HashMap<Integer,Integer> ageCountMap = new HashMap<>();

        try (AgeInputIterator ageInputIterator = iteratorFactory.apply(region)) {
            while(ageInputIterator.hasNext())
            {
                int age = ageInputIterator.next();
                if(age >= 0)
                {
                    ageCountMap.put(age,ageCountMap.getOrDefault(age,0)+1);
                }
                else throw new IllegalArgumentException("Age is invalid");

            }

            List<Integer> keys = ageCountMap.entrySet().stream().sorted(Map.Entry.<Integer, Integer>comparingByValue().reversed()).limit(3).map(Map.Entry::getKey).collect(Collectors.toList());

            for(int i =0;i<keys.size();i++)
            {
                top3Ages.add(String.format(OUTPUT_FORMAT,i+1,keys.get(i),ageCountMap.get(keys.get(i))));

            }

            return top3Ages.toArray(new String[0]);
        }catch (IOException ioException)
        {
            throw  new RuntimeException("Error occured while closing iterator");
        }catch (RuntimeException re){
            if(re instanceof IllegalArgumentException)
                throw new RuntimeException(re.getMessage());
            return new String[]{};
            // region not found

        }
    }

    /**
     * Given a list of region names, call {@link #iteratorFactory} to get an iterator for each region and return
     * the 3 most common ages across all regions in the format specified by {@link #OUTPUT_FORMAT}.
     * We expect you to make use of all cores in the machine, specified by {@link #CORES).
     */
    public String[] top3Ages(List<String> regionNames) {
        List<String> top3Ages = new ArrayList<>();
        HashMap<Integer,Integer> ageCountMap = new HashMap<>();

        for(String region :regionNames) {
            try (AgeInputIterator ageInputIterator = iteratorFactory.apply(region)) {
                while (ageInputIterator.hasNext()) {
                    int age = ageInputIterator.next();
                    if (age >= 0) {
                        ageCountMap.put(age, ageCountMap.getOrDefault(age, 0) + 1);
                    } else throw new IllegalArgumentException("Age is invalid");

                }


            } catch (IOException ioException) {
                throw new RuntimeException("Error occurred while closing iterator");
            } catch (RuntimeException re) {
                if (re instanceof IllegalArgumentException)
                    throw new RuntimeException(re.getMessage());
                // region not found

            }
        }


        List<Integer> keys = ageCountMap.entrySet().stream().sorted(Map.Entry.<Integer, Integer>comparingByValue().reversed()).limit(3).map(Map.Entry::getKey).collect(Collectors.toList());

        for (int i = 0; i < keys.size(); i++) {
            top3Ages.add(String.format(OUTPUT_FORMAT, i + 1, keys.get(i), ageCountMap.get(keys.get(i))));

        }

        return top3Ages.toArray(new String[0]);

    }


    /**
     * Implementations of this interface will return ages on call to {@link Iterator#next()}. They may open resources
     * when being instantiated created.
     */
    public interface AgeInputIterator extends Iterator<Integer>, Closeable {
    }
}
