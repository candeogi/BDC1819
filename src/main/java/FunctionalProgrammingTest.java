import java.io.FileNotFoundException;
import java.util.Arrays;
import java.util.List;

public class FunctionalProgrammingTest {
    public static void main(String[] args){
        List<Integer> values = Arrays.asList(1,2,3,4,5,6,7,8);
        System.out.println(values.stream()
                .filter(e -> e == 2)
                .findAny()
                .get());
    }
}
