import java.util.ArrayList;
import java.util.Collections;

public class ProcesoLE implements Runnable{

    private int id;

    public ProcesoLE(int id){
        this.id=id;
    }

    public void run(){

        for (int k=0; k<10; k++) {

            int accion = (Math.random() < 0.5) ? 0 : 1; // 0 -> lectura | 1 -> escritura

            if (accion == 0){

                BaseDeDatos.lectura.acquireUninterruptibly();

                int tabla = (Math.random() < 0.5) ? 0 : 1;
                int tamanio = BaseDeDatos.obtenerTamanio(tabla);
                if (tamanio != 0){
                    int id = (int) (tamanio * Math.random());
                    ArrayList<Integer> valor = BaseDeDatos.leer(tabla, id);
                    System.out.println(id+". Lectura | tabla: "+tabla+" | id: "+id+" | Valor leido: "+valor.getLast());

                    BaseDeDatos.randomDelay(1, 3);
                }else{
                    System.out.println("No hay datos para leer.");
                }

                BaseDeDatos.lectura.release();
            }else{
                if (accion == 1){

                    BaseDeDatos.escritura.acquireUninterruptibly();
                    for (int i=0; i<3; i++) BaseDeDatos.lectura.acquireUninterruptibly();
                    
                    int valor = (int)(100 * Math.random());
                    System.out.print(id+". Escritura para ");
                    int operacion = (Math.random() < 0.5) ? 0 : 1; // 0 -> sumar | 1 -> actualizar
                    if (operacion == 0){
                        // BD.addAndGet(valor);
                        int tabla = (Math.random() < 0.5) ? 0 : 1;
                        ArrayList<Integer> registro = new ArrayList<Integer>(Collections.nCopies(5, 0));
                        BaseDeDatos.insertar(tabla, registro);
                        System.out.println("insetar | tabla: "+tabla+" | Nuevo valor: "+valor);
                    }else{
                        // BD.set(valor);
                        int tabla = (Math.random() < 0.5) ? 0 : 1;
                        int tamanio = BaseDeDatos.obtenerTamanio(tabla);
                        // int id = (int) (tamanio * Math.random());
                        // BaseDeDatos.actualizar(tabla, 0, id, valor);
                        // System.out.println("actualziar | tabla: "+tabla+" | id:"+id+" | Nuevo valor: "+valor);

                        if (tamanio != 0){
                            int id = (int) (tamanio * Math.random());
                            BaseDeDatos.actualizar(tabla, 0, id, valor);
                            System.out.println("actualziar | tabla: "+tabla+" | id:"+id+" | Nuevo valor: "+valor);

                            BaseDeDatos.randomDelay(1, 3);
                        }else{
                            System.out.println("No hay datos para actualizar.");
                        }
                    }
                    // System.out.println("Nuevo dato: "+BD);

                    BaseDeDatos.randomDelay(3, 6);

                    BaseDeDatos.escritura.release();
                    for (int i=0; i<3; i++) BaseDeDatos.lectura.release();
                }else{
                    System.out.println(id+". Acción no reconocida");
                }
            }

        }

    }
    
}
