package ua.es.central.web;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;

import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpExchange;

public class PanelWeb {
    private final HttpServer servidor;
    private final EstadoPanel panel;
    private final OrdenesKafka ordenes;

    // Archivos del panel que se pueden pedir y su tipo (están en src/main/resources/web/)
    private static final Map<String, String[]> ARCHIVOS = Map.of(
        "/",           new String[]{"/web/panel.html", "text/html; charset=utf-8"},
        "/panel.css",  new String[]{"/web/panel.css",  "text/css; charset=utf-8"},
        "/panel.js",   new String[]{"/web/panel.js",   "text/javascript; charset=utf-8"}
    );

    public PanelWeb(int puerto, EstadoPanel panel, OrdenesKafka ordenes) throws IOException {
        this.panel = panel;
        this.ordenes = ordenes;

        servidor = HttpServer.create(new InetSocketAddress(puerto), 0);
        servidor.createContext("/", this::pagina);
        servidor.createContext("/api/estado", this::estado);
        servidor.createContext("/api/orden", this::orden);
        servidor.setExecutor(Executors.newFixedThreadPool(4));   // varias peticiones a la vez
    }

    public void arrancar() {
        servidor.start();
        System.out.println("Panel web en http://localhost:" + servidor.getAddress().getPort());
    }

    // GET /, panel.css y panel.js -> archivos del panel
    private void pagina(HttpExchange ex) throws IOException {
        String ruta = ex.getRequestURI().getPath();
        String[] archivo = ARCHIVOS.get(ruta);

        if(archivo == null) {
            responder(ex, 404, "text/plain; charset=utf-8", "No encontrado");
            return;
        }

        try(InputStream in = PanelWeb.class.getResourceAsStream(archivo[0])) {
            if(in == null) {
                responder(ex, 500, "text/plain; charset=utf-8", "Falta src/main/resources" + archivo[0]);
                return;
            }
            String contenido = new String(in.readAllBytes(), StandardCharsets.UTF_8);
            responder(ex, 200, archivo[1], contenido);
        }
    }

    // GET /api/estado -> JSON 
    private void estado(HttpExchange ex) throws IOException {
        if(!ex.getRequestMethod().equals("GET")) {
            responder(ex, 405, "text/plain; charset=utf-8", "Método no permitido");
            return;
        }
        responder(ex, 200, "application/json; charset=utf-8", panel.json());
    }

    // POST /api/orden <- accion, idWS, duracion 
    private void orden(HttpExchange ex) throws IOException {
        if(!ex.getRequestMethod().equals("POST")) {
            responderOrden(ex, 405, false, "Método no permitido");
            return;
        }

        String cuerpo = new String(ex.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
        Map<String, String> p = leerFormulario(cuerpo);

        String accion = p.getOrDefault("accion", "");
        String idWS = p.getOrDefault("idWS", "");

        // A qué estaciones va la orden: una o todas 
        List<String> destinos;
        if(idWS.equals("TODAS")) {
            destinos = panel.ids();
        } else if(panel.existe(idWS)) {
            destinos = List.of(idWS);
        } else {
            responderOrden(ex, 404, false, "La estación " + idWS + " no existe");
            return;
        }

        String texto = idWS.equals("TODAS") ? "todas las estaciones" : idWS;

        switch(accion) {
            case "INICIAR": {
                int duracion;
                try {
                    duracion = Integer.parseInt(p.getOrDefault("duracion", ""));
                } catch(NumberFormatException e) {
                    responderOrden(ex, 400, false, "Duración no válida");
                    return;
                }
                if(duracion <= 0) {
                    responderOrden(ex, 400, false, "La duración debe ser mayor que 0");
                    return;
                }
                for(String id : destinos) {
                    ordenes.iniciar(id, duracion);
                }
                responderOrden(ex, 200, true, "Solicitud de riego enviada a " + texto);
                break;
            }
            case "BLOQUEAR":
                for(String id : destinos) {
                    ordenes.bloquear(id);
                }
                responderOrden(ex, 200, true, "Orden de bloqueo enviada a " + texto);
                break;
            case "ACTIVAR":
                for(String id : destinos) {
                    ordenes.activar(id);
                }
                responderOrden(ex, 200, true, "Orden de activación enviada a " + texto);
                break;
            default:
                responderOrden(ex, 400, false, "Acción desconocida: " + accion);
        }
    }

    // Lee "accion=INICIAR&idWS=WS-01&duracion=30" y lo convierte en un mapa
    private static Map<String, String> leerFormulario(String cuerpo) {
        Map<String, String> mapa = new HashMap<>();
        for(String par : cuerpo.split("&")) {
            int igual = par.indexOf('=');
            if(igual <= 0) {
                continue;
            }
            String clave = URLDecoder.decode(par.substring(0, igual), StandardCharsets.UTF_8);
            String valor = URLDecoder.decode(par.substring(igual + 1), StandardCharsets.UTF_8);
            mapa.put(clave, valor);
        }
        return mapa;
    }

    // Respuesta JSON para /api/orden: {"ok":true,"mensaje":"..."}
    private static void responderOrden(HttpExchange ex, int codigo, boolean ok, String mensaje) throws IOException {
        String json = "{\"ok\":" + ok + ",\"mensaje\":\"" + mensaje.replace("\\", "\\\\").replace("\"", "\\\"") + "\"}";
        responder(ex, codigo, "application/json; charset=utf-8", json);
    }

    // Envía una respuesta HTTP con su código, tipo y contenido
    private static void responder(HttpExchange ex, int codigo, String tipo, String cuerpo) throws IOException {
        byte[] bytes = cuerpo.getBytes(StandardCharsets.UTF_8);
        ex.getResponseHeaders().set("Content-Type", tipo);
        ex.getResponseHeaders().set("Cache-Control", "no-store");
        ex.sendResponseHeaders(codigo, bytes.length);
        try(OutputStream os = ex.getResponseBody()) {
            os.write(bytes);
        }
    }
}
