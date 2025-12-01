import java.util.ArrayList;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class BaseDeDatos {
	
	private final ArrayList<ArrayList<Integer>> tabla1 = new ArrayList<>();
	private final ArrayList<ArrayList<Integer>> tabla2 = new ArrayList<>();

	private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

	public void randomDelay(float min, float max){
		int random = (int)(max * Math.random() + min);
		try {
			Thread.sleep(random * 10);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	public void actualizar(int tabla, int column, int id, Integer valor){
        lock.writeLock().lock();
        try {
            ArrayList<ArrayList<Integer>> tablaRef = (tabla == 0) ? tabla1 : tabla2;
            for (ArrayList<Integer> fila : tablaRef) {
                if (fila.get(0) == id) {
                    fila.set(column, valor);
                    break;
                }
            }
        } finally {
            lock.writeLock().unlock();
        }
	}

	public void insertar(int tabla, ArrayList<Integer> registro){
        lock.writeLock().lock();
        try {
            if (tabla == 0) tabla1.add(registro);
            else tabla2.add(registro);
        } finally {
            lock.writeLock().unlock();
        }
	}
	
	public void borrar(int tabla, int id){
        lock.writeLock().lock();
        try {
            ArrayList<ArrayList<Integer>> tablaRef = (tabla == 0) ? tabla1 : tabla2;
            tablaRef.removeIf(fila -> fila.get(0) == id);
        } finally {
            lock.writeLock().unlock();
        }
	}

	public void drop(int tabla){
		if (tabla == 0){
			tabla1.clear();
		}else{
			tabla2.clear();
		}
	}

	public ArrayList<Integer> leer(int tabla, int id){
        lock.readLock().lock();
        try {
            ArrayList<ArrayList<Integer>> tablaRef = (tabla == 0) ? tabla1 : tabla2;
            for (ArrayList<Integer> fila : tablaRef) {
                if (fila.get(0) == id) {
                    return new ArrayList<>(fila); // copia defensiva
                }
            }
            return new ArrayList<>();
        } finally {
            lock.readLock().unlock();
        }
	}

	public ArrayList<Integer> leer_fila(int tabla, int id){
		try {
			if (tabla == 0){
				return tabla1.get(id);
			}else {
				return tabla2.get(id);
			}
		}catch (IndexOutOfBoundsException e) {
			return new ArrayList<>();
		}
	}

	public int obtenerTamanio(int tabla){
		if (tabla == 0){
			return tabla1.size();
		}else{
			return tabla2.size();
		}
	}

	@Override
	public String toString(){
		System.out.println("Tabla 1:");
		for (ArrayList<Integer> fila : tabla1) {
			System.out.println(fila);
		}
		System.out.println("Tabla 2:");
		for (ArrayList<Integer> fila : tabla2) {
			System.out.println(fila);
		}
		return "";
	}

}
