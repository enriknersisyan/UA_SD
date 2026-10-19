package ua.es.central.basedatos;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;


//Consultas sobre la tabla operario. 
public class OperarioBD {

    private final Connection conexion;
    public OperarioBD(Database baseDatos) {
        this.conexion = baseDatos.getConexion();
    }
    // devuelve si existe operario en ese id
    public boolean existe(String id) throws SQLException {
        synchronized (conexion) {
            String sql = "SELECT 1 FROM operario WHERE id = ?";
            try (PreparedStatement ps = conexion.prepareStatement(sql)) {
                ps.setString(1, id);
                try (ResultSet rs = ps.executeQuery()) {
                    return rs.next();
                }
            }
        }
    }

    /**
     * Inserta un operario. Si el id ya existe no hace nada ni da error.
     */
    public void insertar(String id, String nombre) throws SQLException {
        synchronized (conexion) {
            String sql = "INSERT OR IGNORE INTO operario (id, nombre) VALUES (?, ?)";
            try (PreparedStatement ps = conexion.prepareStatement(sql)) {
                ps.setString(1, id);
                ps.setString(2, nombre);
                ps.executeUpdate();
            }
        }
    }
}