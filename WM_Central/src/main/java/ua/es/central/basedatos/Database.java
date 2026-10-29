package ua.es.central.basedatos;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.sql.SQLException;



public class Database{
	private static final String RUTA_POR_DEFECTO = "wm_central.basedatos";
	private static final String VARIABLE_RUTA = "WM_DB_PATH";
	private final Connection conexion;


	/**
	 * Crea la conexion a la base de datos. Si no existe el fichero se crea.
	 * @throws SQLException si hay un error al abrir la base de datos.
	 */
	public Database() throws SQLException{
		String ruta = System.getenv(VARIABLE_RUTA);
		if (ruta == null || ruta.isBlank()){
			ruta = RUTA_POR_DEFECTO;// si no se infica ruta o es vacia se asigna por defecto
		}
		this.conexion = DriverManager.getConnection("jdbc:sqlite:" + ruta);
		System.out.println("Base de datos abierta: " + ruta);
		crearTablas();

	}
	/**
	* Crea las tablas si no existen. No borra ni modifica las existentes.
	 */
	private void crearTablas() throws SQLException{
		try (Statement sentencia = conexion.createStatement()) {
            sentencia.execute(
                "CREATE TABLE IF NOT EXISTS operario ("
                + "id TEXT PRIMARY KEY, "
                + "nombre TEXT NOT NULL)");

            sentencia.execute(
                "CREATE TABLE IF NOT EXISTS estacion ("
                + "id TEXT PRIMARY KEY, "
                + "ubicacion TEXT NOT NULL, "
                + "estado TEXT NOT NULL)");
        }
	}
	public Connection getConexion(){
		return conexion;
	}
	public void cerrar() throws SQLException{
		conexion.close();
	}
}