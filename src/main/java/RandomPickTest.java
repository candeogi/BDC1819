import java.util.ArrayList;
import java.util.concurrent.ThreadLocalRandom;

/**
 * This is a class to test an algorithm used to pick random elements from an ArrayList with weighted probabilities
 */
public class RandomPickTest {
    public static void main(String[] args) {
        // P set of points
        ArrayList<String> P = new ArrayList<>();
        P.add("zero");
        P.add("uno");
        P.add("due");
        P.add("tre");
        P.add("quattro");
        P.add("cinque");
        P.add("sei");
        P.add("sette");
        P.add("otto");
        P.add("nove");

        //weights of P
        ArrayList<Integer> WP = new ArrayList<>();
        WP.add(2);
        WP.add(1);
        WP.add(1);
        WP.add(1);
        WP.add(1);
        WP.add(1);
        WP.add(10);
        WP.add(1);
        WP.add(1);
        WP.add(1);

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

        System.out.println("POINT "+myPoint+" HAS BEEN CHOSEN (index = "+randomNum+")");

        S.add(myPoint);
        WS.add(myWeight);

        //remove the center from P list
        P.remove(randomNum);
        WP.remove(randomNum);


        //choose k-1 remaining centers with probability based on weight (and distance)
        for(int i = 2; i <=k; i++){
            System.out.println("-------start cycle ----------");


            double sum=0;
            //random number between 0 and 1
            double randomPivot = ThreadLocalRandom.current().nextDouble(0, 1);
            System.out.println("randomPivot: "+randomPivot );
            //for each point in "P-S" lets compute the range that will choose him over another
            for(int j = 0; j < P.size(); j++){
                sum =  sum + distance(P.get(j),S)*WP.get(j);
            }


            double currentRange = 0;
            int chosenIndex = 0;
            boolean indexIsChosen = false;

            //choose the random point
            for(int j = 0; j < P.size(); j++){
                double probOfChoosingJ = (distance(P.get(j),S)*WP.get(j) / sum);
                System.out.println("probOfChoosing "+P.get(j)+" is "+probOfChoosingJ);
                System.out.print("currentRange :"+currentRange);
                currentRange = currentRange + probOfChoosingJ;
                System.out.println(" - "+currentRange+" ");
                if((currentRange >= randomPivot)&&(!indexIsChosen)){
                    System.out.println("currentRange >= randomPivot");
                    chosenIndex = j;
                    indexIsChosen = true;
                }
            }
            System.out.println("currentrange should be 1 " +currentRange);

            myPoint = P.get(chosenIndex);
            System.out.println("POINT "+myPoint+" HAS BEEN CHOSEN (index = "+chosenIndex+")");
            myWeight  = WP.get(chosenIndex);

            S.add(myPoint);

            P.remove(chosenIndex);
            WP.remove(chosenIndex);

            chosenIndex = 0;
            System.out.println("-------end cycle ----------");
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
