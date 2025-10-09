import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.Semaphore;

public class Orquestador {

	private static Random rand = new Random();
	public static Semaphore escritura = new Semaphore(1, true);
	public static Semaphore permisoLectura = new Semaphore(1, true);
	public static Semaphore mutex = new Semaphore(1, true);
	public static Semaphore lectoresMaximos = new Semaphore(3, true);
	public static Semaphore backup = new Semaphore(1, true);
	
	public static int cantidadLectores = 0;
	public static int cantidadEscritores = 0;
	public static int backupCount = 0;


	public static int[] secuencia_id_tabla = {3, 5}; // empieza en 3 porque hay 3 registros iniciales en tabla 1 y 5 registros iniciales
    
	public static void main(String[] args) {

		BaseDeDatos principal = new BaseDeDatos();
		BaseDeDatos backup = new BaseDeDatos();
		
		
		/*
		Estructura de las tablas:
		Tabla 1 (índice 0 en el ArrayList de tablas):
		- 3 columnas: Clave primaria (columna 0), Clave foránea a tabla2 (columna 1), Dato entero (columna 2)
		- 3 registros iniciales
		Tabla 2 (índice 1 en el ArrayList de tablas):
		- 2 columnas: Clave primaria (columna 0), Dato entero (columna 1)
		- 5 registros iniciales
		*/
		insertar_en_tabla1(principal);

		//Tabla 2
		// 5 registros iniciales
		// Clave primaria en la columna 0
		// Dato entero en la columna 1
		insertar_en_tabla2(principal);

        
		System.out.println("Base de datos principal inicial:");
		System.out.println(principal);
		System.out.println("Base de datos backup inicial (vacía):");
		System.out.println(backup);
		
		BackUp backupThread = new BackUp(principal, backup);
		GestorConsistencia gc = new GestorConsistencia(principal);

		Thread back = new Thread(backupThread, "BackUp");
		back.setDaemon(true);
		back.start();
		System.out.println("Proceso de backup iniciado.");
		
		Thread gestorCons =new Thread(gc, "Gestor Consistencia");
		gestorCons.setDaemon(true);
		gestorCons.start();
		System.out.println("Proceso de gestor de consistencia iniciado.");


		System.out.println("Iniciando procesos de lectura y escritura...");
		for(int i=0; i<50; i++){
			ProcesoLE proceso = null;
			int tableId = rand.nextInt(2);
			int tamanioTabla;
			switch(elegirAccion()){
				case LECTURA:
						
					proceso = new ProcesoLE(principal, Accion.LECTURA, tableId, 0, 0, 0);
					System.out.println(i+" Lectura");
					break;
				case ESCRITURA:
					tamanioTabla = principal.obtenerTamanio(tableId);
					int valor;
					int columnId; // no se puede modificar la clave primaria
					if(tamanioTabla>0){
						if(tableId == 0){
							columnId = rand.nextInt(2)+1;
							if(columnId == 1){ // modifica la foreing key
								valor = rand.nextInt(tamanioTabla);
							}else{
								valor = rand.nextInt(1000);
							}
						}else{
							columnId = 1; // solo tiene una columna de datos esta tabla
							valor = rand.nextInt(1000);
						}
						proceso = new ProcesoLE(principal, Accion.ESCRITURA , tableId , columnId,  valor, 0);
					}
					System.out.println(i+" Escritura");
					break;
				case INSERCION:
					if(tableId == 0){
						int nuevoValorForeingKey = rand.nextInt(principal.obtenerTamanio(1)+2); // puede ser un valor inválido
						int nuevoValor = rand.nextInt(1000);
						proceso= new ProcesoLE(principal, Accion.INSERCION , tableId ,0 , nuevoValor, nuevoValorForeingKey);
					}else{
						int nuevoValor = rand.nextInt(1000);
						proceso= new ProcesoLE(principal, Accion.INSERCION , tableId , 0, nuevoValor, 0);
					}
					System.out.println(i+" Incercion");
					break;
				case ELIMINACION:
					if(tableId == 0){
						proceso = new ProcesoLE(principal, Accion.ELIMINACION, tableId, 0, 0, 0);
					}
					if(tableId == 1){
						proceso = new ProcesoLE(principal, Accion.ELIMINACION, tableId, 0, 0, 0);
					}
					System.out.println(i+" Eliminar");
					break;
				default:
			}

			if(proceso != null) new Thread(proceso, "Usuario: "+i).start();

			else System.out.println("No se ha podido crear el proceso.");

			try{
				Thread.sleep(500);
			}catch(Exception e){}
		}
		System.out.println("Finalizando la creación procesos de lectura y escritura...");
		System.out.println();
    }
	
	public static Accion elegirAccion(){
		double rand = Math.random();
		if(rand < 0.50) return Accion.LECTURA;
		else if(rand < 0.60) return Accion.ESCRITURA;
		else if(rand < 0.80) return Accion.INSERCION;
		else return Accion.ELIMINACION;
	}
	
	public static void insertar_en_tabla1(BaseDeDatos bd){
		// Registro 0: PK=0, FK=0, Dato=100
		ArrayList<Integer> fila1_0 = new ArrayList<>();
		fila1_0.add(0); // clave primaria
		fila1_0.add(0); // clave foranea a tabla2
		fila1_0.add(100); // dato inicial
		bd.insertar(0,fila1_0);

		// Registro 1: PK=1, FK=1, Dato=200
		ArrayList<Integer> fila1_1 = new ArrayList<>();
		fila1_1.add(1); // clave primaria
		fila1_1.add(7); // clave foranea a tabla2
		fila1_1.add(200); // dato inicial
		bd.insertar(0,fila1_1);

		// Registro 2: PK=2, FK=2, Dato=300
		ArrayList<Integer> fila1_2 = new ArrayList<>();
		fila1_2.add(2); // clave primaria
		fila1_2.add(2); // clave foranea a tabla2
		fila1_2.add(300); // dato inicial
		bd.insertar(0,fila1_2);
	}

	public static void insertar_en_tabla2(BaseDeDatos bd){
		// Registro 0: PK=0, Dato=1000
		ArrayList<Integer> fila2_0 = new ArrayList<>();
		fila2_0.add(0); // clave primaria
		fila2_0.add(1000); // dato inicial
		bd.insertar(1,fila2_0);

		// Registro 1: PK=1, Dato=2000
		ArrayList<Integer> fila2_1 = new ArrayList<>();
		fila2_1.add(1); // clave primaria
		fila2_1.add(2000); // dato inicial
		bd.insertar(1,fila2_1);

		// Registro 2: PK=2, Dato=3000
		ArrayList<Integer> fila2_2 = new ArrayList<>();
		fila2_2.add(2); // clave primaria
		fila2_2.add(3000); // dato inicial
		bd.insertar(1,fila2_2);

		// Registro 3: PK=3, Dato=4000
		ArrayList<Integer> fila2_3 = new ArrayList<>();
		fila2_3.add(3); // clave primaria
		fila2_3.add(4000); // dato inicial
		bd.insertar(1,fila2_3);

		// Registro 4: PK=4, Dato=5000
		ArrayList<Integer> fila2_4 = new ArrayList<>();
		fila2_4.add(4); // clave primaria
		fila2_4.add(5000); // dato inicial
		bd.insertar(1,fila2_4);
	}

}