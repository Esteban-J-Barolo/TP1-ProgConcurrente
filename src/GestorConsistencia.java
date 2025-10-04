public class GestorConsistencia implements Runnable {

    public void run(){

        while (true) {

            BaseDeDatos.escritura.acquireUninterruptibly();
            for (int i=0; i<3; i++) BaseDeDatos.lectura.acquireUninterruptibly();

            System.out.println("Iniciando chequeo de consistencia ...");
            // metodo de chequeo
            BaseDeDatos.randomDelay(400, 700);
            System.out.println("Fin chequeo de consistencia");

            BaseDeDatos.escritura.release();
            for (int i=0; i<3; i++) BaseDeDatos.lectura.release();

        }

    }
    
}
