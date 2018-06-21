import java.util.Random;

public class CreateShell {
    public static void main(String[] args) {
        
        Random random = new Random();

        int id = random.nextInt(Constants.ID_NUM);

        for(int i = 0; i < 100; i++){

            String startRow = Utils.getRowKeyOf16(id);
            String stopRow = Utils.getNextRowKeyOf16(id);

            System.out.println("get 'PerformanceTestMultiVersion'" + ", '" + startRow + "'");
        }
    }
}
