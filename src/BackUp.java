import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;

public class BackUp implements Runnable{

    private ArrayList<ArrayList<AtomicInteger>> tabla;

    public BackUp(ArrayList<ArrayList<AtomicInteger>> tabla){
        this.tabla=tabla;
    }

    public void run(){

        while (true) {

            BaseDeDatos.randomDelay(10, 20);

            System.out.println("Iniciando backup...");

            // System.out.println("Valor guradado: "+BD);

            System.out.println("backup finalizado");
            
        }

    }
    
}
