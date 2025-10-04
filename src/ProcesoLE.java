import java.util.ArrayList;
import java.util.Collections;


public class ProcesoLE implements Runnable{

    public enum Accion {LECTURA, ESCRITURA, INSERCION};
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
                ArrayList<Integer> fila = new ArrayList<Integer>(Collections.nCopies(3, 0));
                fila.set(0, row_id);
                fila.set(1, nuevoValorForeingKey);
                fila.set(2, nuevoValor);
                bd.escritura.acquireUninterruptibly();
                for (int i=0; i<3; i++) bd.lectura.acquireUninterruptibly();
                bd.insertar(0,fila);
                System.out.println(row_id+". Inserción | Nuevo valor: "+nuevoValor+" | Clave foránea: "+nuevoValorForeingKey);
                for (int i=0; i<3; i++) bd.lectura.release();
                bd.escritura.release();
            }else{
                ArrayList<Integer> fila = new ArrayList<Integer>(Collections.nCopies(2, 0));
                fila.set(0, row_id);
                fila.set(1, nuevoValor);
                bd.escritura.acquireUninterruptibly();
                for (int i=0; i<3; i++) bd.lectura.acquireUninterruptibly();
                bd.insertar(1,fila);
                System.out.println(row_id+". Inserción | Nuevo valor: "+nuevoValor);
                for (int i=0; i<3; i++) bd.lectura.release();
                bd.escritura.release();
            }
        }
    }

    private void leer(int table_id, int row_id){
        bd.lectura.acquireUninterruptibly();
        ArrayList<Integer> valor = bd.leer(table_id, row_id);
        System.out.println(row_id+". Lectura | Valor leido: "+valor.getLast());
        bd.lectura.release();
    }

    private void escribir(int table_id, int row_id, int column_id, int nuevoValor){
        bd.escritura.acquireUninterruptibly();
        for (int i=0; i<3; i++) bd.lectura.acquireUninterruptibly();
        bd.actualizar(table_id, column_id, row_id, nuevoValor);
        System.out.println(row_id+". Escritura | Nuevo valor: "+nuevoValor);
        for (int i=0; i<3; i++) bd.lectura.release();
        bd.escritura.release();
    }
    
}
