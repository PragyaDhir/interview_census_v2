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



//        In the example below, the top three are ages 10, 15 and 12
//        return new String[]{
//                String.format(OUTPUT_FORMAT, 1, 10, 38),
//                String.format(OUTPUT_FORMAT, 2, 15, 35),
//                String.format(OUTPUT_FORMAT, 3, 12, 30)
//        };

        //throw new UnsupportedOperationException();
    }

    /**
     * Given a list of region names, call {@link #iteratorFactory} to get an iterator for each region and return
     * the 3 most common ages across all regions in the format specified by {@link #OUTPUT_FORMAT}.
     * We expect you to make use of all cores in the machine, specified by {@link #CORES).
     */
    public String[] top3Ages(List<String> regionNames) {
        String [] top3Ages = new String[3];

        for(String region :regionNames)
        {
            top3Ages(region);
        }

//        In the example below, the top three are ages 10, 15 and 12
//        return new String[]{
//                String.format(OUTPUT_FORMAT, 1, 10, 38),
//                String.format(OUTPUT_FORMAT, 2, 15, 35),
//                String.format(OUTPUT_FORMAT, 3, 12, 30)
//        };

        return  top3Ages;
    }


    /**
     * Implementations of this interface will return ages on call to {@link Iterator#next()}. They may open resources
     * when being instantiated created.
     */
    public interface AgeInputIterator extends Iterator<Integer>, Closeable {
    }
}
