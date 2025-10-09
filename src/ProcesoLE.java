import java.util.ArrayList;
import java.util.Collections;
import java.util.Random;

public class ProcesoLE implements Runnable{

    private Accion accion;
    private int nuevoValor;
    private BaseDeDatos bd;
    private int table_id;
    private int column_id;
    private int nuevoValorForeingKey;

    private static Random rand = new Random();

    public ProcesoLE(BaseDeDatos bd, Accion accion, int table_id,int column_id,int nuevoValor, int nuevoValorForeingKey){
        this.bd = bd;
        this.accion = accion;
        this.nuevoValor = nuevoValor;
        this.table_id = table_id;
        this.nuevoValorForeingKey = nuevoValorForeingKey;
        this.column_id = column_id;
    }

    public void run(){
        int tamanioTabla;
        int row_id;
        if(accion == Accion.LECTURA){
            Orquestador.permisoLectura.acquireUninterruptibly(); // lector pide permiso para entrar (bloquea si hay escritores)
            Orquestador.lectoresMaximos.acquireUninterruptibly(); // ocupa un cupo de lectura
            Orquestador.mutex.acquireUninterruptibly();
            Orquestador.cantidadLectores++;
            if (Orquestador.cantidadLectores == 1) {
                Orquestador.mutex.release();
                Orquestador.escritura.acquireUninterruptibly(); // primer lector bloquea escritores
            }else{
                Orquestador.mutex.release();
            }
            
            Orquestador.permisoLectura.release();

            // ---- Sección crítica de lectura ----

            tamanioTabla = bd.obtenerTamanio(table_id);

            if (tamanioTabla == 0) {

                System.out.println(Thread.currentThread().getName()+": La tabla "+table_id+" está vacía, no hay registros.");
                
                Orquestador.mutex.acquireUninterruptibly();
                Orquestador.cantidadLectores--;
                if (Orquestador.cantidadLectores == 0) Orquestador.escritura.release(); // último lector libera escritores
                Orquestador.mutex.release();
                Orquestador.lectoresMaximos.release(); // libera el cupo al salir
                return;
            }

            row_id = rand.nextInt(tamanioTabla);
            this.leer(table_id, row_id);
            
            // ---- Fin sección crítica de lectura ----

            Orquestador.mutex.acquireUninterruptibly();
            Orquestador.cantidadLectores--;
            if (Orquestador.cantidadLectores == 0) Orquestador.escritura.release(); // último lector libera escritores
            Orquestador.mutex.release();
            Orquestador.lectoresMaximos.release(); // libera el cupo al salir

        }else if(accion == Accion.ESCRITURA){
            Orquestador.mutex.acquireUninterruptibly();
            Orquestador.cantidadEscritores++;
            if (Orquestador.cantidadEscritores == 1) {
                Orquestador.mutex.release();
                Orquestador.permisoLectura.acquireUninterruptibly(); // primer escritor bloquea lectores
            }else {
                Orquestador.mutex.release();
            }

            Orquestador.escritura.acquireUninterruptibly(); // escritor pide permiso para entrar (bloquea si hay lectores)

            // ---- Sección crítica de escritura ----

            tamanioTabla = bd.obtenerTamanio(table_id);
            if (tamanioTabla == 0) {

                System.out.println(Thread.currentThread().getName()+": La tabla "+table_id+" está vacía, no hay registros.");
                
                Orquestador.mutex.acquireUninterruptibly();
                Orquestador.cantidadEscritores--;
                if (Orquestador.cantidadEscritores == 0) Orquestador.permisoLectura.release(); // último escritor libera lectores
                Orquestador.mutex.release();
                Orquestador.escritura.release(); // libera el permiso al salir
                return;
            }
            row_id = rand.nextInt(tamanioTabla);
            this.escribir(table_id, row_id, column_id, nuevoValor);
            Orquestador.escritura.release(); // libera el permiso al salir

            Orquestador.mutex.acquireUninterruptibly();
            Orquestador.cantidadEscritores--;
            if (Orquestador.cantidadEscritores == 0) Orquestador.permisoLectura.release(); // último escritor libera lectores
            Orquestador.mutex.release();

        }else if(accion == Accion.INSERCION){
            Orquestador.mutex.acquireUninterruptibly();
            Orquestador.cantidadEscritores++;
            if (Orquestador.cantidadEscritores == 1) {
                Orquestador.mutex.release();
                Orquestador.permisoLectura.acquireUninterruptibly(); // primer escritor bloquea lectores
            }else {
                Orquestador.mutex.release();
            }

            Orquestador.escritura.acquireUninterruptibly(); // escritor pide permiso para entrar (bloquea si hay lectores)

            // ---- Sección crítica de escritura ----

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
            Orquestador.mutex.acquireUninterruptibly();
            Orquestador.cantidadEscritores++;
            if (Orquestador.cantidadEscritores == 1) {
                Orquestador.mutex.release();
                Orquestador.permisoLectura.acquireUninterruptibly(); // primer escritor bloquea lectores
            }else {
                Orquestador.mutex.release();
            }

            Orquestador.escritura.acquireUninterruptibly(); // escritor pide permiso para entrar (bloquea si hay lectores)

            // ---- Sección crítica de escritura ----

            tamanioTabla = bd.obtenerTamanio(table_id);
            if (tamanioTabla == 0) {

                System.out.println(Thread.currentThread().getName()+": La tabla "+table_id+" está vacía, no hay registros.");
                
                Orquestador.mutex.acquireUninterruptibly();
                Orquestador.cantidadEscritores--;
                if (Orquestador.cantidadEscritores == 0) Orquestador.permisoLectura.release(); // último escritor libera lectores
                Orquestador.mutex.release();
                Orquestador.escritura.release(); // libera el permiso al salir
                return;
            }
            row_id = rand.nextInt(tamanioTabla);
            this.eliminar(table_id, row_id);
            
            Orquestador.escritura.release(); // libera el permiso al salir

            Orquestador.mutex.acquireUninterruptibly();
            Orquestador.cantidadEscritores--;
            if (Orquestador.cantidadEscritores == 0) Orquestador.permisoLectura.release(); // último escritor libera lectores
            Orquestador.mutex.release();

        }
    }

    private void leer(int table_id, int row_id){
        ArrayList<Integer> valor = bd.leer(table_id, row_id);
        if(valor.isEmpty()){
            System.out.println(Thread.currentThread().getName()+": Lectura | Row: "+ row_id+" No existe, tabla "+table_id);
        }else{
            System.out.println(Thread.currentThread().getName()+": Lectura | Row: "+ row_id+" tabla "+table_id+" Valor leido: "+valor.get(1));
        }
    }

    private void escribir(int table_id, int row_id, int column_id, int nuevoValor){
        if(bd.leer(table_id, row_id).isEmpty()){
            System.out.println(Thread.currentThread().getName()+": Escritura | Row: "+ row_id+" No existe, tabla "+table_id);

            return;
        }
        bd.actualizar(table_id, column_id, row_id, nuevoValor);
        System.out.println(Thread.currentThread().getName()+": Escritura | Row: "+ row_id +" Nuevo valor: "+nuevoValor+", tabla "+table_id);
    }
    
    private void insertar_en_tabla1(int row_id, int nuevoValor, int nuevoValorForeingKey){
        ArrayList<Integer> fila = new ArrayList<Integer>(Collections.nCopies(3, 0));
        fila.set(0, row_id);
        fila.set(1, nuevoValorForeingKey);
        fila.set(2, nuevoValor);
        bd.insertar(0,fila);
        System.out.println(Thread.currentThread().getName()+": Inserción | Nuevo valor: "+nuevoValor+" | Clave foránea: "+nuevoValorForeingKey+" id:"+row_id);
    }

    private void insertar_en_tabla2(int row_id, int nuevoValor){
        ArrayList<Integer> fila = new ArrayList<Integer>(Collections.nCopies(2, 0));
        fila.set(0, row_id);
        fila.set(1, nuevoValor);
        bd.insertar(1,fila);
        System.out.println(Thread.currentThread().getName()+": Inserción | Nuevo valor: "+nuevoValor);
    }
    private void eliminar(int table_id, int row_id){
        bd.borrar(table_id, row_id);
        System.out.println(Thread.currentThread().getName()+": Eliminación | Fila: "+row_id+". Registro eliminado de la tabla "+table_id);
    }
    
}
