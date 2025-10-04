import java.util.ArrayList;// 
public class Orquestador {
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

		new Thread(backupThread).start();
		System.out.println("Proceso de backup iniciado.");
		try{
			Thread.sleep(5000);
		}catch(Exception e){

		}
		principal.lectura.acquireUninterruptibly();
		System.out.println("Base de datos backup después del primer backup:");
		System.out.println(backup);
		principal.lectura.release();
		
		new Thread(gc).start();
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

		
    }

}