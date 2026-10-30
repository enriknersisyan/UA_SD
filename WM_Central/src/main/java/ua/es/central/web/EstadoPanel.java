package ua.es.central.web;

import java.time.format.DateTimeFormatter;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

// Estado que se muestra en el Panel de la Central
public class EstadoPanel {
    // Una fila del panel
    private static class Fila {
        final String id;
        String ubicacion;
        String estado = "DESCONECTADA";
        double caudal = 0;
        double volumen = 0;
        String operario = null;

        Fila(String id, String ubicacion) {
            this.id = id;
            this.ubicacion = ubicacion;
        }
    }

    // Constantes
    private static final int MAX_EVENTOS = 100; 
    private static final DateTimeFormatter HORA = DateTimeFormatter.ofPattern("HH:mm:ss");

    private final Map<String, Fila> filas = new TreeMap<>();      // ordenadas por ID
    private final Deque<String[]> eventos = new ArrayDeque<>();   // {id, hora, texto}, el más antiguo primero
    private long siguienteEvento = 1;

    // Carga desde BD
    public synchronized void cargar(String id, String ubicacion) {
        filas.put(id, new Fila(id, ubicacion));
    }

    // Comprobar si existe una fila con esa id
    public synchronized boolean existe(String id) {
        return filas.containsKey(id);
    }

    public synchronized List<String> ids() {
        return new ArrayList<>(filas.keySet());
    }

    // Cambiar estado
    public synchronized void cambiarEstado(String id, String estado) {
        Fila f = filas.get(id);

        if(f == null) {
            return;
        }

        f.estado = estado;

        if(!estado.equals("REGANDO")) {
            f.caudal = 0;
            f.operario = null;
        }
    }

    // Pasa de fuga a disponible
    public synchronized void funFuga(String id) {
        Fila f = filas.get(id);

        if(f != null && f.estado.equals("FUGA")) {
            f.estado = "DISPONIBLE";
        }
    }

    
}