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

            Orquestador.mutexBackUp.acquireUninterruptibly(); // protege la variable backupCount
            Orquestador.backupCount++;
            if (Orquestador.backupCount == 1) Orquestador.permisoEscritura.acquireUninterruptibly();
            Orquestador.mutexBackUp.release();

            Orquestador.backUp.acquireUninterruptibly(); // pide permiso para hacer backup

            hacer_backup(principal, backup); // realiza el backup

            Orquestador.backUp.release();

            Orquestador.mutexBackUp.acquireUninterruptibly(); // protege la variable backupCount
            Orquestador.backupCount--;
            if (Orquestador.backupCount == 0) Orquestador.permisoEscritura.release();
            Orquestador.mutexBackUp.release();
            
            try {
                Thread.sleep(5000); // Esperar 5 segundos antes del próximo backup
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

    }

    private static void hacer_backup(BaseDeDatos principal, BaseDeDatos backup){
            
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

        try{
            Thread.sleep(1500);
        }catch(Exception e){}

        System.out.println("Backup Finalizado");
        
    }
    
}
