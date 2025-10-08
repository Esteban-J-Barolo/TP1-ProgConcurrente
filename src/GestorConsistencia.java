import java.util.ArrayList;

public class GestorConsistencia implements Runnable {
    private BaseDeDatos bd;
    public GestorConsistencia(BaseDeDatos bd){
        this.bd = bd;
    }
    public void run(){

        while (true) {
            
            System.out.println("Iniciando chequeo de consistencia ...");
            // metodo de chequeo de consistencia

            int tabla1 = 0; // tabla 1
            int tabla2 = 1; // tabla 2
            ArrayList<Integer> filasEliminar = new ArrayList<>();
            //obtengo la lista de pks en la tabla 2
            ArrayList<Integer> pks = new ArrayList<>();
            //Inicio protocolo de entrada en zona de lectura de gestor de consistencia
            bd.readTry.acquireUninterruptibly();
            //Fin protocolo de entrada en zona de lectura de gestor de consistencia
            int tamanioTabla2 = bd.obtenerTamanio(tabla2);
            if(tamanioTabla2 != 0){          
                for(int i=0; i<tamanioTabla2; i++){
                    ArrayList<Integer> fila = bd.leer_fila(tabla2, i);
                    int pk = fila.get(0);
                    pks.add(pk);
                }
            }
            int tamanioTabla1 = bd.obtenerTamanio(tabla1);
            
            if(tamanioTabla1 != 0){
                for(int i=0; i<tamanioTabla1; i++){
                    ArrayList<Integer> fila = bd.leer_fila(tabla1, i);
                    int fk = fila.get(1); //fk propagada en la tabla 2
                    if(pks.contains(fk)){
                        // la fk existe en la tabla 2, todo bien
                        System.out.println("Chequeo de consistencia: OK - FK "+fk+" existe en tabla 2");
                    }else{
                        // la fk no existe en la tabla 2, hay que eliminar el registro de la tabla 1
                        System.out.println("Chequeo de consistencia: ERROR - FK "+fk+" no existe en tabla 2. Eliminando registro en tabla 1.");
                        filasEliminar.add(i);
                    }
                    
                }
            }
            if(!filasEliminar.isEmpty()){
                //Inicio protocolo de entrada en zona de escritura de gestor de consistencia
                bd.write.acquireUninterruptibly();
                //Fin protoclo de entrada en zona de escritura del gestor de consistencia
                for(int fila: filasEliminar){
                    bd.borrar(tabla1, fila);
                    System.out.println("Registro "+fila+" eliminado de tabla 1 por inconsistencia.");
                }
                //Inicio protocolo de salida de zona de escritura de gestor de consistencia
                bd.write.release();
                //Fin protocolo de salida de zona de escritura de gestor de consistencia
            }
            //Inicio protoclo de salida en zona de lectura de gestor de consistencia
            bd.readTry.release();
            //Fin protoclo de salida en zona de lectura de gestor de consistencia

            System.out.println("Fin chequeo de consistencia");
            try {
                Thread.sleep(10000); // espera 10 segundos antes de la proxima verificacion
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        }
    
    }
}
