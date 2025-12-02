import java.util.ArrayList;
import java.util.Collections;

public class GestorConsistencia implements Runnable {

    private BaseDeDatos bd;

    public GestorConsistencia(BaseDeDatos bd){
        this.bd = bd;
    }

    public void run(){

        while (true) {
            Orquestador.mutexPriEscritores.acquireUninterruptibly(); // evita starvation de escritores
            Orquestador.permisoLectura.acquireUninterruptibly(); // pide permiso para leer
            Orquestador.mutexLectura.acquireUninterruptibly(); // protege la variable cantidadLectores
            Orquestador.cantidadLectores++;
            if (Orquestador.cantidadLectores == 1) Orquestador.escritura.acquireUninterruptibly(); // primer lector bloquea escritores
            Orquestador.mutexLectura.release();
            Orquestador.permisoLectura.release();
            Orquestador.mutexPriEscritores.release();

            ArrayList<Integer> filasEliminar = chequeo_de_consistencia_lectura();

            if(filasEliminar.isEmpty()){
                System.out.println("Fin chequeo de consistencia");
                System.out.println(bd);
            }

            Orquestador.mutexLectura.acquireUninterruptibly(); // protege la variable cantidadLectores
            Orquestador.cantidadLectores--;
            if (Orquestador.cantidadLectores == 0) Orquestador.escritura.release(); // último lector libera escritores
            Orquestador.mutexLectura.release();

            
            if(!filasEliminar.isEmpty()){
                
                Orquestador.mutexPriBackUp.acquireUninterruptibly(); // evita starvation de backup
                Orquestador.permisoEscritura.acquireUninterruptibly(); // pide permiso para escribir
                Orquestador.mutexEscritura.acquireUninterruptibly(); // protege la variable cantidadEscritores
                Orquestador.cantidadEscritores++;
                if (Orquestador.cantidadEscritores == 1) Orquestador.permisoLectura.acquireUninterruptibly(); // primer escritor bloquea lectores
                Orquestador.mutexEscritura.release();
                Orquestador.permisoEscritura.release();
                Orquestador.mutexPriBackUp.release();

                Orquestador.escritura.acquireUninterruptibly(); // pide permiso para escribir

                eliminar_filas_inconsistentes(filasEliminar);

                System.out.println("Fin chequeo de consistencia");
                System.out.println(bd);

                Orquestador.escritura.release();

                Orquestador.mutexEscritura.acquireUninterruptibly();
                Orquestador.cantidadEscritores--;
                if (Orquestador.cantidadEscritores == 0) Orquestador.permisoLectura.release(); // último escritor libera lectores
                Orquestador.mutexEscritura.release();
            }

            
            try {
                Thread.sleep(10000); // espera 10 segundos antes de la proxima verificacion
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        }
    
    }

    private ArrayList<Integer> chequeo_de_consistencia_lectura(){
        System.out.println("Iniciando chequeo de consistencia ...");
        System.out.println(bd);

        int tabla1 = 0; // tabla 1
        int tabla2 = 1; // tabla 2
        int tamanioTabla2 = bd.obtenerTamanio(tabla2);
        //obtengo la lista de pks en la tabla 2
        ArrayList<Integer> pks = new ArrayList<>();
        if(tamanioTabla2 != 0){          
            for(int i=0; i<tamanioTabla2; i++){
                ArrayList<Integer> fila = bd.leer_fila(tabla2, i);
                int pk = fila.get(0);
                pks.add(pk);
            }
        }
        int tamanioTabla1 = bd.obtenerTamanio(tabla1);
        ArrayList<Integer> filasEliminar = new ArrayList<>();
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
        }else{
            System.out.println("Chequeo de consistencia: La tabla 1 está vacía, no hay registros.");
        }
        return filasEliminar;
    }

    private void eliminar_filas_inconsistentes(ArrayList<Integer> filasEliminar){
        Collections.sort(filasEliminar, Collections.reverseOrder());
        for(int fila: filasEliminar){
            int tabla1 = 0; // tabla 1
            bd.borrar(tabla1, fila);
            System.out.println("Registro "+fila+" eliminado de tabla 1 por inconsistencia.");
        }
    }
}
