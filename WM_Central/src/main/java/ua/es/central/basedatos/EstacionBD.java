package ua.es.central.basedatos;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

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
		String sql = "INSERT OR IGNORE INTO estacion (id, ubicacion,estado) VALUES (?, ?, ?)";
		try(PreparedStatement ps = conexion.prepareStatement(sql)){
			ps.setString(1,estacion.id());
			ps.setString(2,estacion.ubicacion());
			ps.setString(3, estacion.estado().name());
			ps.executeUpdate();
		}
	}
	public List<Estacion> listar() throws SQLException{
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