import java.util.ArrayList;

public class BackUp implements Runnable{
    BaseDeDatos principal;
    BaseDeDatos backup;

    public BackUp(BaseDeDatos principal, BaseDeDatos backup){
        this.principal = principal;
        this.backup = backup;
    }

    @Override
    public void run(){

        while (true) {

            principal.escritura.acquireUninterruptibly();
            for (int i=0; i<3; i++) principal.lectura.acquireUninterruptibly();

            backup.escritura.acquireUninterruptibly();
            for (int i=0; i<3; i++) backup.lectura.acquireUninterruptibly();

            System.out.println("Iniciando backup...");

            // Limpiar el backup antes de copiar
            backup.drop(0);
            backup.drop(1);

            // Copiar Tabla 1
            for(int i=0; i<principal.obtenerTamanio(0); i++){
                ArrayList<Integer> fila = principal.leer_fila(0, i);
                if(!fila.isEmpty()){
                    backup.insertar(0, new ArrayList<>(fila));
                }
            }

            System.out.println("Tabla 1 copiada.");

            // Copiar Tabla 2
            for(int i=0; i<principal.obtenerTamanio(1); i++){
                ArrayList<Integer> fila = principal.leer_fila(1, i);
                if(!fila.isEmpty()){
                    backup.insertar(1, new ArrayList<>(fila));
                }
            }
            System.out.println("Tabla 2 copiada.");

            System.out.println("Backup Finalizado");

            backup.escritura.release();
            for (int i=0; i<3; i++) backup.lectura.release();

            principal.escritura.release();
            for (int i=0; i<3; i++) principal.lectura.release();
            
            try {
                Thread.sleep(5000); // Esperar 5 segundos antes del próximo backup
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

    }
    
}
