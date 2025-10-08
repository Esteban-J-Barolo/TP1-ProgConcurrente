import java.util.ArrayList;
import java.util.Collections;
import java.util.Random;

public class ProcesoLE implements Runnable{

    
    // private int row_id;
    private Accion accion;
    private int nuevoValor;
    private BaseDeDatos bd;
    private int table_id;
    private int column_id;
    private int nuevoValorForeingKey;

    private static Random rand = new Random();

    // public ProcesoLE(BaseDeDatos bd, Accion accion, int table_id,int row_id, int column_id,int nuevoValor, int nuevoValorForeingKey){
    public ProcesoLE(BaseDeDatos bd, Accion accion, int table_id,int column_id,int nuevoValor, int nuevoValorForeingKey){
        this.bd = bd;
        this.accion = accion;
        // this.row_id = row_id;
        this.nuevoValor = nuevoValor;
        this.table_id = table_id;
        this.nuevoValorForeingKey = nuevoValorForeingKey;
        this.column_id = column_id;
    }

    public void run(){
        int tamanioTabla;
        int row_id;
        // Orquestador.procesosMaximos.acquireUninterruptibly();
        // try{
            if(accion == Accion.LECTURA){
                // semaforos lectura
                Orquestador.permisoLectura.acquireUninterruptibly(); // lector pide permiso para entrar (bloquea si hay escritores)
                Orquestador.lectoresMaximos.acquireUninterruptibly(); // ocupa un cupo de lectura
                Orquestador.mutex.acquireUninterruptibly();
                Orquestador.cantidadLectores++;
                if (Orquestador.cantidadLectores == 1) Orquestador.escritura.acquireUninterruptibly(); // primer lector bloquea escritores
                Orquestador.mutex.release();
                Orquestador.permisoLectura.release();

                // ---- Sección crítica de lectura ----

                tamanioTabla = bd.obtenerTamanio(table_id);
                row_id = rand.nextInt(tamanioTabla);
                this.leer(table_id, row_id);
                
                // ---- Fin sección crítica de lectura ----

                Orquestador.mutex.acquireUninterruptibly();
                Orquestador.cantidadLectores--;
                if (Orquestador.cantidadLectores == 0) Orquestador.escritura.release(); // último lector libera escritores
                Orquestador.mutex.release();
                Orquestador.lectoresMaximos.release(); // libera el cupo al salir

            }else if(accion == Accion.ESCRITURA){
                // semaforos escritura
                Orquestador.mutex.acquireUninterruptibly();
                Orquestador.cantidadEscritores++;
                if (Orquestador.cantidadEscritores == 1) Orquestador.permisoLectura.acquireUninterruptibly(); // primer escritor bloquea lectores
                Orquestador.mutex.release();

                Orquestador.escritura.acquireUninterruptibly(); // escritor pide permiso para entrar (bloquea si hay lectores)
                // ---- Sección crítica de escritura ----
                tamanioTabla = bd.obtenerTamanio(table_id);
                row_id = rand.nextInt(tamanioTabla);
                this.escribir(table_id, row_id, column_id, nuevoValor);
                Orquestador.escritura.release(); // libera el permiso al salir

                Orquestador.mutex.acquireUninterruptibly();
                Orquestador.cantidadEscritores--;
                if (Orquestador.cantidadEscritores == 0) Orquestador.permisoLectura.release(); // último escritor libera lectores
                Orquestador.mutex.release();

            }else if(accion == Accion.INSERCION){
                // semaforos escritura
                Orquestador.mutex.acquireUninterruptibly();
                System.out.println("mutex 1 ins");
                Orquestador.cantidadEscritores++;
                if (Orquestador.cantidadEscritores == 1) {
                    Orquestador.permisoLectura.acquireUninterruptibly(); // primer escritor bloquea lectores
                    System.out.println("perm lect ins");
                }
                Orquestador.mutex.release();
                System.out.println("mutex 2 ins");

                Orquestador.escritura.acquireUninterruptibly(); // escritor pide permiso para entrar (bloquea si hay lectores)
                if(table_id == 0){
                    this.insertar_en_tabla1(Orquestador.secuencia_id_tabla[table_id], nuevoValor, nuevoValorForeingKey);
                    Orquestador.secuencia_id_tabla[table_id]++;
                }else{
                    this.insertar_en_tabla2(Orquestador.secuencia_id_tabla[table_id], nuevoValor);
                    Orquestador.secuencia_id_tabla[table_id]++;
                }
                Orquestador.escritura.release(); // libera el permiso al salir

                Orquestador.mutex.acquireUninterruptibly();
                Orquestador.cantidadEscritores--;
                if (Orquestador.cantidadEscritores == 0) Orquestador.permisoLectura.release(); // último escritor libera lectores
                Orquestador.mutex.release();

            }else if(accion == Accion.ELIMINACION){
                // semaforos eliminacion
                Orquestador.mutex.acquireUninterruptibly();
                Orquestador.cantidadEscritores++;
                if (Orquestador.cantidadEscritores == 1) Orquestador.permisoLectura.acquireUninterruptibly(); // primer escritor bloquea lectores
                Orquestador.mutex.release();

                Orquestador.escritura.acquireUninterruptibly(); // escritor pide permiso para entrar (bloquea si hay lectores)

                tamanioTabla = bd.obtenerTamanio(table_id);
                row_id = rand.nextInt(tamanioTabla);
                this.eliminar(table_id, row_id);
                
                Orquestador.escritura.release(); // libera el permiso al salir

                Orquestador.mutex.acquireUninterruptibly();
                Orquestador.cantidadEscritores--;
                if (Orquestador.cantidadEscritores == 0) Orquestador.permisoLectura.release(); // último escritor libera lectores
                Orquestador.mutex.release();

            }
        // }finally{
        //     Orquestador.procesosMaximos.release();
        // }
    }

    private void leer(int table_id, int row_id){
        // bd.lectura.acquireUninterruptibly();
        ArrayList<Integer> valor = bd.leer(table_id, row_id);
        // bd.lectura.release();
        if(valor.isEmpty()){
            System.out.println(Thread.currentThread().getName()+": Lectura | Row: "+ row_id+" No existe");
        }else{
            System.out.println(Thread.currentThread().getName()+": Lectura | Row: "+ row_id+" Valor leido: "+valor.get(1));
        }
    }

    private void escribir(int table_id, int row_id, int column_id, int nuevoValor){
        // bd.escritura.acquireUninterruptibly();
        // for (int i=0; i<3; i++) bd.lectura.acquireUninterruptibly();
        if(bd.leer(table_id, row_id).isEmpty()){
            System.out.println(Thread.currentThread().getName()+": Escritura | Row: "+ row_id+" No existe");
            // for (int i=0; i<3; i++) bd.lectura.release();
            // bd.escritura.release();
            return;
        }
        bd.actualizar(table_id, column_id, row_id, nuevoValor);
        System.out.println(Thread.currentThread().getName()+": Escritura | Row: "+ row_id +" Nuevo valor: "+nuevoValor);
        // for (int i=0; i<3; i++) bd.lectura.release();
        // bd.escritura.release();
    }
    
    private void insertar_en_tabla1(int row_id, int nuevoValor, int nuevoValorForeingKey){
        ArrayList<Integer> fila = new ArrayList<Integer>(Collections.nCopies(3, 0));
        fila.set(0, row_id);
        fila.set(1, nuevoValorForeingKey);
        fila.set(2, nuevoValor);
        // bd.escritura.acquireUninterruptibly();
        // for (int i=0; i<3; i++) bd.lectura.acquireUninterruptibly();
        bd.insertar(0,fila);
        System.out.println(Thread.currentThread().getName()+": Inserción | Nuevo valor: "+nuevoValor+" | Clave foránea: "+nuevoValorForeingKey);
        // for (int i=0; i<3; i++) bd.lectura.release();
        // bd.escritura.release();
    }

    private void insertar_en_tabla2(int row_id, int nuevoValor){
        ArrayList<Integer> fila = new ArrayList<Integer>(Collections.nCopies(2, 0));
        fila.set(0, row_id);
        fila.set(1, nuevoValor);
        // bd.escritura.acquireUninterruptibly();
        // for (int i=0; i<3; i++) bd.lectura.acquireUninterruptibly();
        bd.insertar(1,fila);
        System.out.println(Thread.currentThread().getName()+": Inserción | Nuevo valor: "+nuevoValor);
        // for (int i=0; i<3; i++) bd.lectura.release();
        // bd.escritura.release();
    }
    private void eliminar(int table_id, int row_id){
        // bd.escritura.acquireUninterruptibly();
        // for (int i=0; i<3; i++) bd.lectura.acquireUninterruptibly();
        bd.borrar(table_id, row_id);
        System.out.println(Thread.currentThread().getName()+": Eliminación | Fila: "+row_id+". Registro eliminado de la tabla "+table_id);
        // for (int i=0; i<3; i++) bd.lectura.release();
        // bd.escritura.release();
    }
    
}
