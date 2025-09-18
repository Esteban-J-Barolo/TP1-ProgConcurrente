import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.Semaphore;

public class BaseDeDatos {

    static Semaphore lectura = new Semaphore(3);
    static Semaphore escritura = new Semaphore(1);
	static Semaphore accesoBD = new Semaphore(3);

	private static ArrayList<ArrayList<Integer>> tabla1 = new ArrayList<>();
	private static ArrayList<ArrayList<Integer>> tabla2 = new ArrayList<>();

    public static void main(String[] args) {

		int filas = 3;
		int columnas = 4;

		for (int i = 0; i < filas; i++) {
			ArrayList<Integer> fila = new ArrayList<>(Collections.nCopies(columnas, 0));
			tabla1.add(fila);
			tabla2.add(fila);
		}

		for (int j=0; j<2; j++){ // generar varias instacias de BD

			for(int i=0; i<5; i++) new Thread(new ProcesoLE(i+j)).start();

			// new Thread(new BackUp(BD)).start();
		}

    }
    
	public static void randomDelay(float min, float max){
		int random = (int)(max * Math.random() + min);
		try {
			Thread.sleep(random * 10);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	public static void actualizar(int tabla, int column, int id, Integer valor){
		if (tabla == 0){
			tabla1.get(id).set(column, valor);
		}else{
			tabla2.get(id).set(column, valor);
		}
	}

	public static void insertar(int tabla, ArrayList<Integer> registro){
		if (tabla == 0){
			registro.set(0, Integer.valueOf(tabla1.size()));
			tabla1.add(registro);
		}else{
			registro.set(0, Integer.valueOf(tabla2.size()));
			tabla2.add(registro);
		}
	}

	public static ArrayList<Integer> leer(int tabla, int id){
		if (tabla == 0){
			return tabla1.get(id);
		}else{
			return tabla2.get(id);
		}
	}

	public static int obtenerTamanio(int tabla){
		if (tabla == 0){
			return tabla1.size();
		}else{
			return tabla2.size();
		}
	}

}
