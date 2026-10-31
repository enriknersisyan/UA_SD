package ua.es.central.basedatos;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;

import ua.es.central.modelo.Estacion;
import ua.es.central.modelo.Estado;

public class DatosIniciales {
	private static final String FICHERO_POR_DEFECTO = "/datos_iniciales.csv";
	private static final String VARIABLE_RUTA = "WM_DATOS_PATH";


	public static void cargar(EstacionBD estaciones, OperarioBD operarios) throws IOException, SQLException {
		// Las estaciones del CSV solo se cargan la primera vez (BD sin estaciones). Despues se gestionan
		// desde el panel, y asi una estacion borrada no vuelve a aparecer al reiniciar Central.
		boolean cargarEstaciones = estaciones.listar().isEmpty();
		try(BufferedReader lector = abrir()){
			String linea;
			int numLinea = 0;

			while((linea = lector.readLine()) != null){
				numLinea++;
				linea = linea.trim();
				if(linea.isEmpty() || linea.startsWith("#")){
					continue;
				}
				String[] campos = linea.split(";");
				if(campos.length !=3){
					System.out.println("Datos iniciales, linea " + numLinea + " ignorada: " + linea);
					continue;
				}
				String tipo = campos[0].trim();
				String id = campos[1].trim();
				String dato = campos[2].trim();
				if(tipo.equals("ESTACION")){
					if(cargarEstaciones){
						estaciones.insertar(new Estacion(id, dato,Estado.DESCONECTADA));
					}
				}
				else if (tipo.equals("OPERARIO")){
					operarios.insertar(id, dato);
				}
				else {
					System.out.println("Datos iniciales, linea " +numLinea + " con tipo desconocido: " + tipo);
				}
			}
		}
	}



	private static BufferedReader abrir() throws IOException {
		String ruta = System.getenv(VARIABLE_RUTA);
		if(ruta != null && !ruta.isEmpty()){
			System.out.println("Datos iniciales: "+ ruta);
			return Files.newBufferedReader(Path.of(ruta), StandardCharsets.UTF_8);
		}
		InputStream is = DatosIniciales.class.getResourceAsStream(FICHERO_POR_DEFECTO);
		if(is == null){
			throw new IOException("Recurso de datos iniciales no encontrado: " + FICHERO_POR_DEFECTO);
		}
		System.out.println("Datos iniciales: " + FICHERO_POR_DEFECTO);
		return new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8));
	}
}