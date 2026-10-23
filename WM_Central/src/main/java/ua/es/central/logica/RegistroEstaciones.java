package ua.es.central.logica;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import ua.es.central.modelo.Estacion;
import ua.es.central.modelo.Estado;
import ua.es.central.basedatos.EstacionBD;

public class RegistroEstaciones {
	 private final Map<String, Estado> estados = new ConcurrentHashMap<>();
    
     private final EstacionBD bd;
     public RegistroEstaciones(EstacionBD bd) {
        this.bd = bd;
     }
	 
     public void cargar(List<Estacion> estaciones) {
        for(Estacion e : estaciones) {
            estados.put(e.id(), Estado.DESCONECTADA);
        }
    }

    public String registrar(String linea){
        String[] partes = linea.split("#");
        if(partes.length !=2 || !partes[0].equals("REGISTER")) {
            return "KO";
        }
        String id = partes[1];

        //No esta la estacion en al bd
        if(!estados.containsKey(id)) {
            return "KO";
        }
        // si dos monitores llegan gana uno, KO si ya hay uno conectado
        if(!estados.replace(id, Estado.DESCONECTADA, Estado.DISPONIBLE)){
            return "KO";
        }
        
        guardar(id, Estado.DISPONIBLE);
        return "OK";
    }



    public String procesar(String id, String linea) {
        String[] partes = linea.split("#");
        if(partes.length != 2 || !partes[1].equals(id)) {
            return "KO";
        }

        String comando = partes[0];
        switch(comando) {
            case "STATUS":  break;
            case "LEAK":       estados.put(id, Estado.FUGA); guardar(id, Estado.FUGA);      break;
            case "LEAK_FIXED": if(estados.replace(id, Estado.FUGA, Estado.DISPONIBLE)) {
                                    guardar(id, Estado.DISPONIBLE);
                                }      break;


            default:           return "KO";
        }
        return "OK";
    }
    public void desconectar(String id){
        guardar(id, Estado.DESCONECTADA);
        estados.put(id, Estado.DESCONECTADA);
    }


    private void guardar(String id, Estado estado) {
        try {
            bd.actualizarEstado(id, estado);
        } catch (SQLException e) {
            System.out.println("Error al actualizar el estado de la estacion " + id + ": " + e.getMessage());
        }
    }   
}