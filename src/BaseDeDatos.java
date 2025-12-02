import java.util.ArrayList;

public class BaseDeDatos {
	
	private final ArrayList<ArrayList<Integer>> tabla1 = new ArrayList<>();
	private final ArrayList<ArrayList<Integer>> tabla2 = new ArrayList<>();
	private int nextIdTabla1 = 0;
	private int nextIdTabla2 = 0;

	public void randomDelay(float min, float max){
		int random = (int)(max * Math.random() + min);
		try {
			Thread.sleep(random * 10);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	public void actualizar(int tabla, int column, int id, Integer valor){
		ArrayList<ArrayList<Integer>> tablaRef = (tabla == 0) ? tabla1 : tabla2;
		for (ArrayList<Integer> fila : tablaRef) {
			if (fila.get(0) == id) {
				fila.set(column, valor);
				break;
			}
		}
	}

	public void insertar(int tabla, ArrayList<Integer> registro){
		if (tabla == 0){
			tabla1.add(registro);
			nextIdTabla1++;
		}else{
			tabla2.add(registro);
			nextIdTabla2++;
		} 
	}
	
	public void borrar(int tabla, int id){
		ArrayList<ArrayList<Integer>> tablaRef = (tabla == 0) ? tabla1 : tabla2;
		tablaRef.remove(id);
	}

	public void drop(int tabla){
		if (tabla == 0){
			tabla1.clear();
		}else{
			tabla2.clear();
		}
	}

	public ArrayList<Integer> leer(int tabla, int id){
		ArrayList<ArrayList<Integer>> tablaRef = (tabla == 0) ? tabla1 : tabla2;
		for (ArrayList<Integer> fila : tablaRef) {
			if (fila.get(0) == id) {
				return new ArrayList<>(fila);
			}
		}
		return new ArrayList<>();
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

	public int obtenerIds(int tabla){
		if (tabla == 0){
			return nextIdTabla1;
		}else{
			return nextIdTabla2;
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
