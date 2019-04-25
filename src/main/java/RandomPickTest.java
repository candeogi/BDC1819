import javafx.util.Pair;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.concurrent.ThreadLocalRandom;

public class RandomPickTest {
    public static void main(String[] args) {
        // P set of points
        ArrayList<String> P = new ArrayList<>();
        P.add("uno");
        P.add("due");
        P.add("tre");
        P.add("quattro");
        P.add("cinque");
        P.add("sei");
        P.add("sette");
        P.add("otto");
        P.add("nove");
        P.add("dieci");

        //weights of P
        ArrayList<Integer> WP = new ArrayList<>();
        WP.add(1);
        WP.add(2);
        WP.add(3);
        WP.add(4);
        WP.add(5);
        WP.add(6);
        WP.add(7);
        WP.add(8);
        WP.add(9);
        WP.add(10);

        int k = 3;
        int myWeight;
        String myPoint;

        //ArrayList<String> copyOfP = new ArrayList<>(P);
        //ArrayList<Integer> copyOfWP = new ArrayList<>(WP);
        ArrayList<String> S = new ArrayList<>();
        ArrayList<Integer> WS = new ArrayList<>();


        //pick first center
        int randomNum = ThreadLocalRandom.current().nextInt(0, P.size());
        myPoint = P.get(randomNum);
        myWeight =  WP.get(randomNum);

        S.add(myPoint);
        WS.add(myWeight);

        //remove the center from P list
        P.remove(randomNum);
        WP.remove(randomNum);

        double randomPivot = 0;
        double distance = 1;
        double sum = 0;
        int w_p = 1;
        double currentRange = 0;
        int chosenIndex = 0;

        //choose k-1 remaining centers with probability based on weight (and distance)
        for(int i = 2; i <=k; i++){
            //random number between 0 and 1
            randomPivot = ThreadLocalRandom.current().nextDouble(0, P.size());
            //for each point in "P-S" lets compute the range that will choose him over another
            for(int j = 0; j < P.size(); j++){
                sum =  sum + distance(P.get(j),S)*WP.get(j);
            }
            //choose the random point
            for(int j = 0; j < P.size(); j++){
                currentRange = currentRange + (distance(P.get(j),S)*WP.get(j) / sum);
                if(currentRange >= randomPivot){
                    chosenIndex = j;
                    break;
                }
            }
            myPoint = P.get(chosenIndex);
            myWeight  = WP.get(chosenIndex);

            S.add(myPoint);

            P.remove(chosenIndex);
            WP.remove(chosenIndex);

            chosenIndex = 0;
        }

        //print S
        System.out.println("--------------S--------------");
        for(String s : S){
            System.out.println(s);
        }

        //print P
        System.out.println("--------------P--------------");
        for(String p : P){
            System.out.println(p);
        }
    }

    //fake method
    private static double distance(String s, ArrayList<String> s1) {
        return 1;
    }

}
