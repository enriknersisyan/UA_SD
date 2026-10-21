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
    public String procesar(String linea) {
        String[] partes = linea.split("#");
        if(partes.length != 2) {
            return "KO";
        }
        String comando = partes[0];
        String id = partes[1];

        if(!estados.containsKey(id)) {
            return "KO"; // estacion no dada de alta en la BD
        }
        switch(comando) {
            case "REGISTER":   estados.put(id, Estado.DISPONIBLE); break;
            case "LEAK":       estados.put(id, Estado.FUGA);       break;
            case "LEAK_FIXED": estados.replace(id, Estado.FUGA, Estado.DISPONIBLE); break;
            default:           return "KO";
        }
        return "OK";
    }
}