package ua.es.central.logica;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import ua.es.central.modelo.Estacion;
import ua.es.central.modelo.Estado;

public class RegistroEstaciones {
	 private final Map<String, Estado> estados = new ConcurrentHashMap<>();
	 
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
            case "LEAK":       estados.put(id, Estado.FUGA);       break;
            case "LEAK_FIXED": estados.replace(id, Estado.FUGA, Estado.DISPONIBLE); break;
            default:           return "KO";
        }
        return "OK";
    }
    public void desconectar(String id){
        estados.put(id, Estado.DESCONECTADA);
    }
}