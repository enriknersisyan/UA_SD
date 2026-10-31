package ua.es.central.web;

import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
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
        String ultimoRiego = null;   // hora en la que terminó el último riego

        Fila(String id, String ubicacion) {
            this.id = id;
            this.ubicacion = ubicacion;
        }
    }

    // Una petición de riego: RECIBIDA -> AUTORIZADA -> REGANDO -> TERMINADA, o DENEGADA
    private static class Peticion {
        final long id;
        final String hora;
        final String operario;
        final String ws;
        final int duracion;
        String fase = "RECIBIDA";
        String resultado = "";

        Peticion(long id, String hora, String operario, String ws, int duracion) {
            this.id = id;
            this.hora = hora;
            this.operario = operario;
            this.ws = ws;
            this.duracion = duracion;
        }

        boolean abierta() {
            return !fase.equals("TERMINADA") && !fase.equals("DENEGADA");
        }
    }

    // Constantes
    private static final int MAX_EVENTOS = 100;
    private static final int MAX_PETICIONES = 50;
    private static final DateTimeFormatter HORA = DateTimeFormatter.ofPattern("HH:mm:ss");
    private static final DateTimeFormatter FECHA_HORA = DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm:ss");

    private final Map<String, Fila> filas = new TreeMap<>();      // ordenadas por ID
    private final Deque<String[]> eventos = new ArrayDeque<>();   // {id, hora, texto}, el más antiguo primero
    private long siguienteEvento = 1;
    private final List<Peticion> peticiones = new ArrayList<>();  // la más antigua primero
    private long siguientePeticion = 1;
    private final String arranque = LocalDateTime.now().format(FECHA_HORA);
    private Boolean kafka = null;   // null = todavía no se sabe

    // Carga desde BD
    public synchronized void cargar(String id, String ubicacion) {
        filas.put(id, new Fila(id, ubicacion));
    }

    // Quitar la fila de una estación borrada
    public synchronized void quitar(String id) {
        filas.remove(id);
    }

    // Cambiar la ubicación que se muestra
    public synchronized void cambiarUbicacion(String id, String ubicacion) {
        Fila f = filas.get(id);
        if(f != null) {
            f.ubicacion = ubicacion;
        }
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
    public synchronized void finFuga(String id) {
        Fila f = filas.get(id);

        if(f != null && f.estado.equals("FUGA")) {
            f.estado = "DISPONIBLE";
        }
    }

    // Iniciar riego
    public synchronized void iniciarRiego(String id, String operario) {
        Fila f = filas.get(id);

        if(f == null) {
            return;
        }

        f.estado = "REGANDO";
        f.operario = operario;
        f.caudal = 0;
        f.volumen = 0;
    }

    // Información sobre caudal y volumen 
    public synchronized void telemetria(String id, double caudal, double volumen) {
        Fila f = filas.get(id);

        if(f == null || !f.estado.equals("REGANDO")) {
            return;
        }

        f.caudal = caudal;
        f.volumen = volumen;
    }

    // Para acabar el riego, si estaba en FUGA o FUERA_DE_SERVICIO no se hace nada
    public synchronized void funRiego(String id) {
        Fila f = filas.get(id);

        if(f == null) {
            return;
        }

        f.caudal = 0;
        f.operario = null;

        if(f.estado.equals("REGANDO")) {
            f.estado = "DISPONIBLE";
        }
    }

    // Anota si Central consigue o no hablar con Kafka
    public synchronized void kafka(boolean conectado) {
        kafka = conectado;
    }

    // Nueva petición de riego: queda RECIBIDA mientras Central la comprueba
    public synchronized void peticion(String operario, String ws, int duracion) {
        peticiones.add(new Peticion(siguientePeticion++, LocalTime.now().format(HORA), operario, ws, duracion));
        while(peticiones.size() > MAX_PETICIONES) {
            peticiones.remove(0);
        }
    }

    // La petición más reciente de esa estación que aún no ha terminado
    private Peticion abierta(String ws) {
        for(int i = peticiones.size() - 1; i >= 0; i--) {
            Peticion p = peticiones.get(i);
            if(p.ws.equals(ws) && p.abierta()) {
                return p;
            }
        }
        return null;
    }

    // La petición pasa a AUTORIZADA o REGANDO
    public synchronized void fase(String ws, String fase) {
        Peticion p = abierta(ws);
        if(p != null) {
            p.fase = fase;
        }
    }

    // Deniega la petición recién recibida de ese operario para esa estación
    public synchronized void denegada(String ws, String operario, String motivo) {
        for(int i = peticiones.size() - 1; i >= 0; i--) {
            Peticion p = peticiones.get(i);
            if(p.ws.equals(ws) && p.operario.equals(operario) && p.fase.equals("RECIBIDA")) {
                p.fase = "DENEGADA";
                p.resultado = motivo;
                return;
            }
        }
    }

    // El riego ha terminado: se guarda el resumen y la hora del último riego de la estación
    public synchronized void terminada(String ws, String resultado) {
        Peticion p = abierta(ws);
        if(p != null) {
            p.fase = "TERMINADA";
            p.resultado = resultado;
        }
        Fila f = filas.get(ws);
        if(f != null) {
            f.ultimoRiego = LocalTime.now().format(HORA);
        }
    }

    // Mensajes de la aplicación
    public synchronized void anotar(String texto) {
        eventos.addLast(new String[]{
            String.valueOf(siguienteEvento++),
            LocalTime.now().format(HORA),
            texto
        });
        while(eventos.size() > MAX_EVENTOS) {
            eventos.removeFirst();
        }
        System.out.println("[PANEL] " + texto);
    }

    // Generar JSON
     public synchronized String json() {
        StringBuilder sb = new StringBuilder();
        sb.append("{\"central\":{\"kafka\":").append(kafka == null ? "null" : kafka.toString())
          .append(",\"arranque\":").append(texto(arranque)).append("}");
        sb.append(",\"estaciones\":[");

        boolean primero = true;
        for(Fila f : filas.values()) {
            if(!primero) {
                sb.append(',');
            }
            primero = false;
            sb.append("{\"id\":").append(texto(f.id))
              .append(",\"ubicacion\":").append(texto(f.ubicacion))
              .append(",\"estado\":").append(texto(f.estado))
              .append(",\"caudal\":").append(String.format(Locale.US, "%.2f", f.caudal))
              .append(",\"volumen\":").append(String.format(Locale.US, "%.2f", f.volumen))
              .append(",\"operario\":").append(f.operario == null ? "null" : texto(f.operario))
              .append(",\"ultimoRiego\":").append(texto(f.ultimoRiego))
              .append('}');
        }

        sb.append("],\"eventos\":[");
        primero = true;
        for(String[] e : eventos) {
            if(!primero) {
                sb.append(',');
            }
            primero = false;
            sb.append("{\"id\":").append(e[0])
              .append(",\"hora\":").append(texto(e[1]))
              .append(",\"texto\":").append(texto(e[2]))
              .append('}');
        }

        sb.append("],\"peticiones\":[");
        primero = true;
        for(Peticion p : peticiones) {
            if(!primero) {
                sb.append(',');
            }
            primero = false;
            sb.append("{\"id\":").append(p.id)
              .append(",\"hora\":").append(texto(p.hora))
              .append(",\"operario\":").append(texto(p.operario))
              .append(",\"ws\":").append(texto(p.ws))
              .append(",\"duracion\":").append(p.duracion)
              .append(",\"fase\":").append(texto(p.fase))
              .append(",\"resultado\":").append(texto(p.resultado))
              .append('}');
        }
        sb.append("]}");
        return sb.toString();
    }

    // Convierte un String a texto JSON escapando comillas, barras y saltos de línea
    private static String texto(String s) {
        if(s == null) {
            return "null";
        }
        StringBuilder sb = new StringBuilder("\"");
        for(char c : s.toCharArray()) {
            switch(c) {
                case '"':  sb.append("\\\""); break;
                case '\\': sb.append("\\\\"); break;
                case '\n': sb.append("\\n");  break;
                case '\r': sb.append("\\r");  break;
                case '\t': sb.append("\\t");  break;
                default:
                    if(c < 0x20) {
                        sb.append(String.format("\\u%04x", (int) c));
                    } else {
                        sb.append(c);
                    }
            }
        }
        return sb.append('"').toString();
    }
}