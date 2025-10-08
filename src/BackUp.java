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
            //acceso a las variables compartidas
            principal.mutex.acquireUninterruptibly();
            principal.backupCount++;
            if(principal.backupCount == 1){
                //primer backup bloquea entrada de lectores y escritores
                principal.mutex.release();
                principal.readTry.acquireUninterruptibly(); // bloquea nuevos lectores
                principal.write.acquireUninterruptibly(); // bloquea escritores
            } else{
                principal.mutex.release();
            }
            //solicito acceso al backup
            principal.backup.acquireUninterruptibly();

            //Inicia la seccion critica
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
            //final seccion critica
            //libero el semaforo de backup
            principal.backup.release();
            
            //termino el backup
            principal.mutex.acquireUninterruptibly();
            principal.backupCount--;
            if(principal.backupCount == 0){
                principal.write.release();
                principal.readTry.release();
            }
            principal.mutex.release();
            
            try {
                Thread.sleep(5000); // Esperar 5 segundos antes del próximo backup
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

    }
    
}
