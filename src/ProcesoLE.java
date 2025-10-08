import java.util.ArrayList;
import java.util.Collections;

public class ProcesoLE implements Runnable{

    
    private int row_id;
    private Accion accion;
    private int nuevoValor;
    private BaseDeDatos bd;
    private int table_id;
    private int column_id;
    private int nuevoValorForeingKey;
    public ProcesoLE(BaseDeDatos bd, Accion accion, int table_id,int row_id, int column_id,int nuevoValor, int nuevoValorForeingKey){
        this.bd = bd;
        this.accion = accion;
        this.row_id = row_id;
        this.nuevoValor = nuevoValor;
        this.table_id = table_id;
        this.nuevoValorForeingKey = nuevoValorForeingKey;
    }

    public void run(){
        if(accion == Accion.LECTURA){
            this.leer(table_id, row_id);
        }else if(accion == Accion.ESCRITURA){
            this.escribir(table_id, row_id, column_id, nuevoValor);
        }else if(accion == Accion.INSERCION){
            if(table_id == 0){
                this.insertar_en_tabla1(row_id, nuevoValor, nuevoValorForeingKey);
            }else{
                this.insertar_en_tabla2(row_id, nuevoValor);
            }
        }else if(accion == Accion.ELIMINACION){
            this.eliminar(table_id, row_id);
        }
    }

    private void leer(int table_id, int row_id){
        //Comienzo protocolo de lectura
        bd.readTry.acquireUninterruptibly();
        bd.maxLectores.acquireUninterruptibly();
        bd.mutex.acquireUninterruptibly();
        bd.readCount++;
        if(bd.readCount == 1) {
            bd.mutex.release();
            bd.write.acquireUninterruptibly();
        }else bd.mutex.release();
        bd.readTry.release();
        //Comienzo de la sección crítica|
        ArrayList<Integer> valor = bd.leer(table_id, row_id);
        //Fin de la sección crítica|
        //Comienzo protocolo de salida de lectura
        bd.mutex.acquireUninterruptibly();
        bd.readCount--;
        if(bd.readCount==0)
            bd.write.release();
        bd.mutex.release();
        bd.maxLectores.release();
        //Fin protocolo de salida de lectura
        if(valor.isEmpty()){
            System.out.println(Thread.currentThread().getName()+": Lectura | Row: "+ row_id+" No existe");
        }else{
            System.out.println(Thread.currentThread().getName()+": Lectura | Row: "+ row_id+" Valor leido: "+valor.get(1));
        }
    }

    private void escribir(int table_id, int row_id, int column_id, int nuevoValor){
        //Comienzo protocolo de escritura
        bd.mutex.acquireUninterruptibly();
        bd.writeCount++;
        if(bd.writeCount == 1){
            bd.mutex.release();
            bd.readTry.acquireUninterruptibly();
        }else bd.mutex.release();
        bd.write.acquireUninterruptibly();
        //Fin protocolo de escritura
        //Comienzo de la seccion crítica
        if(bd.leer(table_id, row_id).isEmpty()){
            //Fin de la seccion crítica
            //Comienzo protocolo fin escritura
            bd.write.release();
            bd.mutex.acquireUninterruptibly();
            bd.writeCount--;
            if(bd.writeCount == 0) bd.readTry.release();
            bd.mutex.release();
            //Fin protocolo fin escritura
            System.out.println(Thread.currentThread().getName()+": Escritura | Row: "+ row_id+" No existe");
            return;
        }
        bd.actualizar(table_id, column_id, row_id, nuevoValor);
        //Fin de la seccion crítica
        //Comienzo protocolo fin escritura
        bd.write.release();
        bd.mutex.acquireUninterruptibly();
        bd.writeCount--;
        if(bd.writeCount == 0) bd.readTry.release();
        bd.mutex.release();
        //Fin protocolo fin escritura
        System.out.println(Thread.currentThread().getName()+": Escritura | Row: "+ row_id +" Nuevo valor: "+nuevoValor);
    }
    
    private void insertar_en_tabla1(int row_id, int nuevoValor, int nuevoValorForeingKey){
        ArrayList<Integer> fila = new ArrayList<Integer>(Collections.nCopies(3, 0));
        fila.set(0, row_id);
        fila.set(1, nuevoValorForeingKey);
        fila.set(2, nuevoValor);
        //Comienzo protocolo de escritura
        bd.mutex.acquireUninterruptibly();
        bd.writeCount++;
        if(bd.writeCount == 1){
            bd.mutex.release();
            bd.readTry.acquireUninterruptibly();
        }else bd.mutex.release();
        bd.write.acquireUninterruptibly();
        //Fin protocolo de escritura
        //Comienzo de la seccion crítica
        bd.insertar(0,fila);
        //Fin de la seccion crítica
        //Comienzo protocolo fin escritura
        bd.write.release();
        bd.mutex.acquireUninterruptibly();
        bd.writeCount--;
        if(bd.writeCount == 0) bd.readTry.release();
        bd.mutex.release();
        //Fin protocolo fin escritura
        System.out.println(Thread.currentThread().getName()+": Inserción | Nuevo valor: "+nuevoValor+" | Clave foránea: "+nuevoValorForeingKey);
    }

    private void insertar_en_tabla2(int row_id, int nuevoValor){
        ArrayList<Integer> fila = new ArrayList<Integer>(Collections.nCopies(2, 0));
        fila.set(0, row_id);
        fila.set(1, nuevoValor);
        //Comienzo protocolo de escritura
        bd.mutex.acquireUninterruptibly();
        bd.writeCount++;
        if(bd.writeCount == 1){
            bd.mutex.release();
            bd.readTry.acquireUninterruptibly();
        }else bd.mutex.release();
        bd.write.acquireUninterruptibly();
        //Fin protocolo de escritura
        //Comienzo de la seccion crítica
        bd.insertar(1,fila);
        //Fin de la seccion crítica
        //Comienzo protocolo fin escritura
        bd.write.release();
        bd.mutex.acquireUninterruptibly();
        bd.writeCount--;
        if(bd.writeCount == 0) bd.readTry.release();
        bd.mutex.release();
        //Fin protocolo fin escritura
        System.out.println(Thread.currentThread().getName()+": Inserción | Nuevo valor: "+nuevoValor);
    }
    private void eliminar(int table_id, int row_id){
        //Comienzo protocolo de escritura
        bd.mutex.acquireUninterruptibly();
        bd.writeCount++;
        if(bd.writeCount == 1){
            bd.mutex.release();
            bd.readTry.acquireUninterruptibly();
        }else bd.mutex.release();
        bd.write.acquireUninterruptibly();
        //Fin protocolo de escritura
        //Comienzo de la seccion crítica
        bd.borrar(table_id, row_id);
        System.out.println(row_id+". Eliminación | Registro eliminado de la tabla "+table_id);
        //Fin de la seccion crítica
        //Comienzo protocolo fin escritura
        bd.write.release();
        bd.mutex.acquireUninterruptibly();
        bd.writeCount--;
        if(bd.writeCount == 0) bd.readTry.release();
        bd.mutex.release();
        //Fin protocolo fin escritura
    }
    
}
