import java.util.ArrayList;
// import java.util.concurrent.Semaphore;

public class BaseDeDatos {

    // public Semaphore lectura = new Semaphore(3, true);
    // public Semaphore escritura = new Semaphore(1, true);


    //public Semaphore accesoBD = new Semaphore(1);
	
	private final ArrayList<ArrayList<Integer>> tabla1 = new ArrayList<>();
	private final ArrayList<ArrayList<Integer>> tabla2 = new ArrayList<>();

	public void randomDelay(float min, float max){
		int random = (int)(max * Math.random() + min);
		try {
			Thread.sleep(random * 10);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	public void actualizar(int tabla, int column, int id, Integer valor){
		if (tabla == 0){
			// tabla1.get(id).set(column, valor);
			tabla1.stream()
					.filter(fila -> fila.get(0) == id)
					.findFirst()
					.ifPresent(fila -> fila.set(column, valor));
		}else{
			// tabla2.get(id).set(column, valor);
			tabla2.stream()
					.filter(fila -> fila.get(0) == id)
					.findFirst()
					.ifPresent(fila -> fila.set(1, valor));
		}
	}

	public void insertar(int tabla, ArrayList<Integer> registro){
		if (tabla == 0){
			// registro.set(0, tabla1.size());
			tabla1.add(registro);
		}else{
			// registro.set(0, tabla2.size());
			tabla2.add(registro);
		}
	}
	
	public void borrar(int tabla, int id){
		if (tabla == 0){
			for(ArrayList<Integer> fila : tabla1) {
				if (fila.get(0) == id) {
					tabla1.remove(fila);
					break;
				}
			}
		}else{
			for(ArrayList<Integer> fila : tabla2) {
				if (fila.get(0) == id) {
					tabla2.remove(fila);
					break;
				}
			}
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
		if (tabla == 0){
			for(ArrayList<Integer> fila : tabla1) {
				if (fila.get(0) == id) {
					return fila;
				}
			}
		}else{
			for(ArrayList<Integer> fila : tabla2) {
				if (fila.get(0) == id) {
					return fila;
				}
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
