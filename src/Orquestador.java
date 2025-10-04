import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.Semaphore;
public class Orquestador {
	private static Random rand = new Random();
	public static Semaphore procesosMaximos = new Semaphore(6, true);
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

		ArrayList<Integer> fila1_0 = new ArrayList<>();
		fila1_0.add(0); // clave primaria
		fila1_0.add(0); // clave foránea a tabla2
		fila1_0.add(100); // dato inicial
		principal.insertar(0, fila1_0);
		

		// Registro 1: PK=1, FK=1, Dato=200
		ArrayList<Integer> fila1_1 = new ArrayList<>();
		fila1_1.add(1); // clave primaria
		fila1_1.add(7); // clave foránea a tabla2
		fila1_1.add(200); // dato inicial
		principal.insertar(0,fila1_1);

		// Registro 2: PK=2, FK=2, Dato=300
		ArrayList<Integer> fila1_2 = new ArrayList<>();
		fila1_2.add(2); // clave primaria
		fila1_2.add(2); // clave foránea a tabla2
		fila1_2.add(300); // dato inicial
		principal.insertar(0,fila1_2);


		//Tabla 2
		// 5 registros iniciales
		// Clave primaria en la columna 0
		// Dato entero en la columna 1

		// Registro 0: PK=0, Dato=10
		ArrayList<Integer> fila2_0 = new ArrayList<>();
		fila2_0.add(0); // clave primaria
		fila2_0.add(10); // dato inicial
		principal.insertar(1,fila2_0);

		// Registro 1: PK=1, Dato=20
		ArrayList<Integer> fila2_1 = new ArrayList<>();
		fila2_1.add(1); // clave primaria
		fila2_1.add(20); // dato inicial
		principal.insertar(1,fila2_1);

		// Registro 2: PK=2, Dato=30
		ArrayList<Integer> fila2_2 = new ArrayList<>();
		fila2_2.add(2); // clave primaria
		fila2_2.add(30); // dato inicial
		principal.insertar(1,fila2_2);

		// Registro 3: PK=3, Dato=40
		ArrayList<Integer> fila2_3 = new ArrayList<>();
		fila2_3.add(3); // clave primaria
		fila2_3.add(40); // dato inicial
		principal.insertar(1,fila2_3);

		// Registro 4: PK=4, Dato=50
		ArrayList<Integer> fila2_4 = new ArrayList<>();
		fila2_4.add(4); // clave primaria
		fila2_4.add(50); // dato inicial
		principal.insertar(1,fila2_4);
        
		System.out.println("Base de datos principal inicial:");
		System.out.println(principal);
		System.out.println("Base de datos backup inicial (vacía):");
		System.out.println(backup);
		
		BackUp backupThread = new BackUp(principal, backup);
		GestorConsistencia gc = new GestorConsistencia(principal);

		new Thread(backupThread, "BackUp").start();
		System.out.println("Proceso de backup iniciado.");
		try{
			Thread.sleep(5000);
		}catch(Exception e){

		}
		principal.lectura.acquireUninterruptibly();
		System.out.println("Base de datos backup después del primer backup:");
		System.out.println(backup);
		principal.lectura.release();
		
		new Thread(gc, "Gestor Consistencia").start();
		System.out.println("Proceso de gestor de consistencia iniciado.");
		try{
			Thread.sleep(5000);
		}catch(Exception e){

		}
		System.out.println("Bases de datos después de la primera verificación de consistencia:");
		principal.lectura.acquireUninterruptibly();
		System.out.println("Base de datos principal:");
		System.out.println(principal);
		System.out.println("Base de datos backup:");
		System.out.println(backup);
		principal.lectura.release();


		System.out.println("Iniciando procesos de lectura y escritura...");
		for(int i=0; i<50; i++){
			ProcesoLE proceso = null;
			int tableId;
			int rowId;
			int tamanioTabla;
			switch(elegirAccion()){
				case LECTURA:
					tableId = rand.nextInt(2);
					principal.lectura.acquireUninterruptibly();
					tamanioTabla = principal.obtenerTamanio(tableId);
					principal.lectura.release();
					if(tamanioTabla>0){
						rowId = rand.nextInt(tamanioTabla);
						principal.lectura.release();
						if(tableId == 0){
							proceso = new ProcesoLE(principal, Accion.LECTURA, tableId, rowId, 0, 0, 0);
						}
						if(tableId == 1){
							proceso = new ProcesoLE(principal, Accion.LECTURA, tableId, rowId, 0, 0, 0);
						}
						break;
					}
				case ESCRITURA:
					tableId = rand.nextInt(2);
					principal.lectura.acquireUninterruptibly();
					tamanioTabla = principal.obtenerTamanio(tableId);
					principal.lectura.release();
					if(tamanioTabla>0){
						rowId = rand.nextInt(tamanioTabla);
						principal.lectura.release();
						int valor;
						int columnId;
						if(tableId == 0){
							columnId = rand.nextInt(2)+1; // no se puede modificar la clave primaria
							if(columnId==1){ // modifica la foreing key
							principal.lectura.acquireUninterruptibly();
							valor = rand.nextInt(principal.obtenerTamanio(tableId)); 
							principal.lectura.release();
							}else{
								valor = rand.nextInt(1000);
							}
						}else{
							columnId = 1; // solo tiene una columna de datos esta tabla
							valor = rand.nextInt(1000);
						}
						proceso = new ProcesoLE(principal, Accion.ESCRITURA , tableId , rowId , columnId,  valor, 0);
						break;
					}
				case INSERCION:
					tableId = rand.nextInt(2);
					if(tableId == 0){
						principal.lectura.acquireUninterruptibly();
						int nuevoValorForeingKey = rand.nextInt(principal.obtenerTamanio(1)+2); // puede ser un valor inválido
						principal.lectura.release();
						int nuevoValor = rand.nextInt(1000);
						proceso= new ProcesoLE(principal, Accion.INSERCION , tableId ,0 , 0, nuevoValor, nuevoValorForeingKey);
					}else{
						int nuevoValor = rand.nextInt(1000);
						proceso= new ProcesoLE(principal, Accion.INSERCION , tableId ,0 , 0, nuevoValor, 0);
					}
					break;
				case ELIMINACION:
					tableId = rand.nextInt(2);
					principal.lectura.acquireUninterruptibly();
					rowId = rand.nextInt(principal.obtenerTamanio(tableId));
					principal.lectura.release();
					if(tableId == 0){
						proceso = new ProcesoLE(principal, Accion.ELIMINACION, tableId, rowId, 0, 0, 0);
					}
					if(tableId == 1){
						proceso = new ProcesoLE(principal, Accion.ELIMINACION, tableId, rowId, 0, 0, 0);
					}
					break;
				default:
			}
			if(proceso != null) new Thread(proceso, "Usuario: "+i).start();
			else System.out.println("No se ha podido crear el proceso.");
			try{
				Thread.sleep(500);
			}catch(Exception e){}
			if(i%5==0 && i!=0){
				principal.lectura.acquireUninterruptibly();
				System.out.println("Mostrando estado de las bases de datos tras varias operaciones:");
				System.out.println("Base de datos principal:");
				System.out.println(principal);
				System.out.println("Base de datos backup:");
				System.out.println(backup);
				principal.lectura.release();
			}
		}
		System.out.println("Finalizando la creación procesos de lectura y escritura...");
		principal.lectura.acquireUninterruptibly();
		System.out.println("Mostrando estado de las bases de datos tras varias operaciones:");
		System.out.println("Base de datos principal:");
		System.out.println(principal);
		System.out.println("Base de datos backup:");
		System.out.println(backup);
		principal.lectura.release();
    }
	
	public static Accion elegirAccion(){
		double rand = Math.random();
		if(rand < 0.25) return Accion.LECTURA;
		else if(rand < 0.5) return Accion.ESCRITURA;
		else if(rand < 0.75) return Accion.INSERCION;
		else return Accion.ELIMINACION;
	}
	

}