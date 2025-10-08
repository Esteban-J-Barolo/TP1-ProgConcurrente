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

            // principal.escritura.acquireUninterruptibly();
            // for (int i=0; i<3; i++) principal.lectura.acquireUninterruptibly();

            // backup.escritura.acquireUninterruptibly();
            // for (int i=0; i<3; i++) backup.lectura.acquireUninterruptibly();

            Orquestador.mutex.acquireUninterruptibly();
            Orquestador.backupCount++;
            if (Orquestador.backupCount == 1) {
                Orquestador.mutex.release();
                Orquestador.permisoLectura.acquireUninterruptibly(); // primer backup bloquea lectores
                Orquestador.escritura.acquireUninterruptibly(); // primer backup bloquea escritores
            }else{
                Orquestador.mutex.release();
            }

            Orquestador.backup.acquireUninterruptibly(); // pide permiso para hacer backup (bloquea si hay otro backup)

            hacer_backup(principal, backup);

            Orquestador.backup.release(); // libera el permiso al salir
            // System.out.println("backup");

            Orquestador.mutex.acquireUninterruptibly();
            // System.out.println("mutex");
            Orquestador.backupCount--;
            if (Orquestador.backupCount == 0) {
                Orquestador.permisoLectura.release(); // último backup libera lectores
                Orquestador.escritura.release(); // último backup libera escritores
            }
            Orquestador.mutex.release();

            // backup.escritura.release();
            // for (int i=0; i<3; i++) backup.lectura.release();

            // principal.escritura.release();
            // for (int i=0; i<3; i++) principal.lectura.release();
            
            try {
                Thread.sleep(5000); // Esperar 5 segundos antes del próximo backup
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

    }

    private static void hacer_backup(BaseDeDatos principal, BaseDeDatos backup){
        // System.out.println("-------------------------");
        System.out.println("Iniciando backup...");

        // System.out.println("Base de datos principal:");
        // System.out.println(principal);
        // System.out.println("Base de datos backup:");
        // System.out.println(backup);

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
        
        // System.out.println("Base de datos principal:");
        // System.out.println(principal);
        // System.out.println("Base de datos backup:");
        // System.out.println(backup);
        // System.out.println("-------------------------");
    }
    
}
