package ua.es.central.basedatos;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import ua.es.central.modelo.Estacion;
import ua.es.central.modelo.Estado;

public class EstacionBD{
	private final Connection conexion;
	//Gauarda la conexion
	public EstacionBD(Database baseDatos){
		this.conexion = baseDatos.getConexion();
	}
	//Guarda una estacion a menos que el id ya este registrado
	public void insertar(Estacion estacion ) throws SQLException {
		synchronized(conexion){
			String sql = "INSERT OR IGNORE INTO estacion (id, ubicacion,estado) VALUES (?, ?, ?)";
			try(PreparedStatement ps = conexion.prepareStatement(sql)){
				ps.setString(1,estacion.id());
				ps.setString(2,estacion.ubicacion());
				ps.setString(3, estacion.estado().name());
				ps.executeUpdate();
			}
		}
		
	}
	public List<Estacion> listar() throws SQLException{
		synchronized(conexion){
			List<Estacion> resultado = new ArrayList<>();
			String sql = "SELECT id, ubicacion, estado FROM estacion";
			try (PreparedStatement ps = conexion.prepareStatement(sql);
				 ResultSet rs = ps.executeQuery()) {
				// Cada vuelta del bucle es una fila de la tabla.
				while (rs.next()) {
					resultado.add(new Estacion(
						rs.getString("id"),
						rs.getString("ubicacion"),
						Estado.valueOf(rs.getString("estado"))));
				}
			}
			return resultado;		
		}
	}

	public void actualizarEstado(String id, Estado estado) throws SQLException {
		synchronized(conexion){
			String sql = "UPDATE estacion SET estado = ? WHERE id = ?";
			try(PreparedStatement ps = conexion.prepareStatement(sql)){
				ps.setString(1, estado.name());
				ps.setString(2, id);
				ps.executeUpdate();
			}
		}
	}
	public void desconectarTodas() throws SQLException {
		synchronized(conexion){
			String sql = "UPDATE estacion SET estado = ?";
			try(PreparedStatement ps = conexion.prepareStatement(sql)){
				ps.setString(1, Estado.DESCONECTADA.name());
				ps.executeUpdate();
			}
		}
	}
	// Borra una estacion. Devuelve si existia
	public boolean borrar(String id) throws SQLException {
		synchronized(conexion){
			String sql = "DELETE FROM estacion WHERE id = ?";
			try(PreparedStatement ps = conexion.prepareStatement(sql)){
				ps.setString(1, id);
				return ps.executeUpdate() > 0;
			}
		}
	}
	// Cambia la ubicacion de una estacion. Devuelve si existia
	public boolean cambiarUbicacion(String id, String ubicacion) throws SQLException {
		synchronized(conexion){
			String sql = "UPDATE estacion SET ubicacion = ? WHERE id = ?";
			try(PreparedStatement ps = conexion.prepareStatement(sql)){
				ps.setString(1, ubicacion);
				ps.setString(2, id);
				return ps.executeUpdate() > 0;
			}
		}
	}
}