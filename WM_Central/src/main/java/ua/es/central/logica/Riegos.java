package ua.es.central.logica;

import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import ua.es.central.web.EstadoPanel;

import ua.es.central.basedatos.OperarioBD;
import ua.es.central.kafka.Productor;
import ua.es.central.modelo.Estado;

import java.util.Locale;

// Reacciona a los mensajes de Kafka que forman un riego:
//   FO -> peticion -> Central reserva la estacion (REGANDO) -> AUTORIZACION -> Engine responde
//   -> AUTORIZADO al FO + INICIAR -> RIEGO_INICIADO -> telemetria -> FIN_RIEGO -> resumen al FO
// Los datos del riego (tiempo, volumen) viven en el Engine. Los mensajes del Engine ya llevan el
// operario, asi que Central solo recuerda quien espera una autorizacion mientras dura esa espera.
public class Riegos {
    private static final String LOCAL = "LOCAL"; // riego sin FO al que avisar (pedido desde la estacion o desde Central)
    private static final int PLAZO_S = 5;        // espera maxima a la respuesta de autorizacion del Engine

    private record Espera(String operario, int duracion) {}

    // Por estacion, quien espera la respuesta del Engine. Quitarla de aqui (remove) decide quien gana
    // entre la respuesta del Engine y el plazo: solo uno de los dos lo consigue.
    private final Map<String, Espera> esperando = new ConcurrentHashMap<>();
    private final ScheduledExecutorService plazos = Executors.newSingleThreadScheduledExecutor();

    // Riegos autorizados y aun sin terminar: estacion -> operario. Sirve para avisar al FO
    // si su estacion se averia o se desconecta en mitad del riego.
    private final Map<String, String> enCurso = new ConcurrentHashMap<>();

    // Recuperacion de riegos que siguen en marcha cuando Central no lo sabe (por ejemplo, tras
    // reiniciarse Central): si llega telemetria de una estacion DISPONIBLE, es que esta regando.
    private static final long SIN_TELEMETRIA_MS = 5000; // un riego recuperado sin telemetria en 5 s se libera
    // Por estacion, la marca (hora del Engine) de su ultimo FIN_RIEGO: la telemetria anterior es vieja
    private final Map<String, Long> ultimoFin = new ConcurrentHashMap<>();
    // Riegos recuperados por telemetria: estacion -> hora (de Central) de su ultima telemetria
    private final Map<String, Long> recuperados = new ConcurrentHashMap<>();

    private final RegistroEstaciones registro;
    private final OperarioBD operarios;
    private final Productor productor;
    private final EstadoPanel panel;


    /**
     * @param registro para cambiar el estado de la estacion
     * @param operarios para comprobar que el operario existe
     * @param productor para enviar mensajes a los FO y al Engine
     */
    public Riegos(RegistroEstaciones registro, OperarioBD operarios, Productor productor, EstadoPanel panel) {
        this.registro = registro;
        this.operarios = operarios;
        this.productor = productor;
        this.panel = panel;
        // Cada segundo se revisan los riegos recuperados que se han quedado sin telemetria
        plazos.scheduleAtFixedRate(this::revisarRecuperados, 1, 1, TimeUnit.SECONDS);
    }

    /**
     * Peticion desde un FO: SOLICITUD_RIEGO#idWS#operario#duracion
     */
    public void peticion(String valor) {
        String[] c = valor.split("#");
        if(c.length == 3 && c[0].equals("TERMINAR_RIEGO")) { // TERMINAR_RIEGO#operario#idWS
            terminarDesdeFO(c[1], c[2]);
            return;
        }
        if(c.length != 4 || !c[0].equals("SOLICITUD_RIEGO")) {
            System.out.println("Peticion mal formada: " + valor);
            return;
        }
        iniciar(c[1], c[2], c[3]);
    }

    /**
     * Un FO pide terminar su riego. Solo puede terminar el suyo: el que Central tiene en curso
     * a su nombre en esa estacion. Si no, se le contesta PARADA_RECHAZADA#idWS#motivo.
     */
    private void terminarDesdeFO(String operario, String idWS) {
        if(!operario.equals(enCurso.get(idWS))) {
            responder(operario, "PARADA_RECHAZADA#" + idWS + "#NO_HAY_RIEGO_TUYO");
            return;
        }
        productor.enviar(Productor.TOPIC_ORDENES, idWS, "TERMINAR#PARADA_FO");
        panel.anotar("Riego [" + operario + "] pide terminar el riego en " + idWS);
    }

    /**
     * Orden desde la consola de Central: se comporta como un operario, sin FO al que avisar
     */
    public void desdeCentral(String idWS, int duracion) {
        iniciar(LOCAL, idWS, String.valueOf(duracion));
    }

    /**
     * Cambio de estado de una estacion (lo avisa el registro). Si se averia o se desconecta
     * durante un riego, se avisa al FO que lo pidio: AVERIA#idWS#estado.
     * El resumen llegara despues, cuando el Engine se recupere y envie su FIN_RIEGO.
     */
    public void cambioEstado(String idWS, Estado estado) {
        if(estado != Estado.FUGA && estado != Estado.DESCONECTADA) {
            return;
        }
        String operario = enCurso.remove(idWS); // remove: se avisa una sola vez
        if(operario != null) {
            responder(operario, "AVERIA#" + idWS + "#" + estado);
        }
    }

    /**
     * topicEventos: lo que cuenta el Engine
     */
    public void evento(String valor, long marca) {
        String[] c = valor.split("#");
        switch(c[0]) {
            case "SOLICITUD_RIEGO":   iniciar(LOCAL, c[1], c[3]); break; // SOLICITUD_RIEGO#idWS#LOCAL#duracion
            case "RESP_AUTORIZACION": respuestaAutorizacion(c);   break;
            case "RIEGO_INICIADO":    panel.fase(c[1], "REGANDO"); System.out.println("Riego iniciado en " + c[1] + " por " + c[2]); break;
            case "FIN_RIEGO":         ultimoFin.merge(c[1], marca, Math::max); // la telemetria anterior ya es vieja
                                      finRiego(c);                break;
            default:                  System.out.println("Evento: " + valor);
        }
    }

    /**
     * topicTelemetria: TELEMETRIA#idWS#caudal#volumen#operario, cada segundo
     */
    public void telemetria(String valor, long marca) {
        String[] c = valor.split("#");
        recuperar(c[1], c[4], marca);
        recuperados.computeIfPresent(c[1], (id, hora) -> System.currentTimeMillis()); // sigue viva
        panel.telemetria(c[1], Double.parseDouble(c[2]), Double.parseDouble(c[3]));
        if(!c[4].equals(LOCAL)) {
            productor.enviar(Productor.TOPIC_RESPUESTAS, c[4], "TELEMETRIA#" + c[1] + "#" + c[2] + "#" + c[3]);
        }
    }
    /**
     * Si llega telemetria de una estacion que Central cree DISPONIBLE, la estacion esta regando sin
     * que Central lo sepa (Central se reinicio, o se corto un momento la conexion con el Monitor):
     * se recupera el riego para que el panel lo vuelva a mostrar.
     */
    private void recuperar(String idWS, String operario, long marca) {
        Long fin = ultimoFin.get(idWS);
        if(fin != null && marca <= fin) {
            return; // telemetria de un riego que ya termino (llega tarde por otro topic): se ignora
        }
        if(!registro.cambiar(idWS, Estado.DISPONIBLE, Estado.REGANDO)) {
            return; // ya estaba REGANDO (riego normal) o en otro estado: no hay nada que recuperar
        }
        recuperados.put(idWS, System.currentTimeMillis());
        if(!operario.equals(LOCAL)) {
            enCurso.put(idWS, operario); // para volver a avisar al FO si hay una averia
        }
        panel.peticion(operario, idWS, 0);     // duracion desconocida: la sabe el Engine
        panel.iniciarRiego(idWS, operario);
        panel.fase(idWS, "REGANDO");
        panel.anotar("Riego en curso recuperado en " + idWS + " (" + operario + ")");
    }

    /**
     * Un riego recuperado que lleva SIN_TELEMETRIA_MS sin telemetria ya no esta en marcha
     * (por ejemplo, se recupero con un mensaje viejo): se libera la estacion.
     */
    private void revisarRecuperados() {
        try {
            long ahora = System.currentTimeMillis();
            for(Map.Entry<String, Long> r : recuperados.entrySet()) {
                String idWS = r.getKey();
                if(ahora - r.getValue() > SIN_TELEMETRIA_MS && recuperados.remove(idWS, r.getValue())) {
                    enCurso.remove(idWS);
                    if(registro.cambiar(idWS, Estado.REGANDO, Estado.DISPONIBLE)) {
                        panel.terminada(idWS, "sin telemetria: riego recuperado liberado");
                        panel.anotar("Riego recuperado en " + idWS + " sin telemetria: se libera la estacion");
                    }
                }
            }
        } catch(RuntimeException e) { // una tarea periodica que falla deja de repetirse sin avisar
            System.out.println("Error revisando riegos recuperados: " + e);
        }
    }

    /**
     * Inicia un riego: reserva la estacion y pide autorizacion al Engine
     */
    private void iniciar(String operario, String idWS, String duracionTxt) {
        int duracion;
        try {
            duracion = Integer.parseInt(duracionTxt);
        } catch(NumberFormatException e) {
            duracion = 0;
        }
        panel.peticion(operario, idWS, duracion);
        responder(operario, "RECIBIDA#" + idWS);

        if(duracion <= 0) {
            denegar(operario, idWS, "DURACION_INVALIDA");
            return;
        }
        try {
            if(!operario.equals(LOCAL) && !operarios.existe(operario)) {
                denegar(operario, idWS, "OPERARIO_DESCONOCIDO");
                return;
            }
        } catch(SQLException e) {
            denegar(operario, idWS, "ERROR_INTERNO");
            return;
        }
        // Reserva: la estacion pasa a REGANDO en un solo paso atomico. Si otra peticion llega despues,
        // ya no esta DISPONIBLE y rebota.
        if(!registro.cambiar(idWS, Estado.DISPONIBLE, Estado.REGANDO)) {
            denegar(operario, idWS, registro.estado(idWS) == Estado.FUGA ? "FUGA" : "NO_DISPONIBLE");
            return;
        }

        panel.iniciarRiego(idWS, operario);
        Espera espera = new Espera(operario, duracion);
        esperando.put(idWS, espera);
        productor.enviar(Productor.TOPIC_ORDENES, idWS, "AUTORIZACION#" + operario + "#" + duracion);
        vigilar(idWS, espera);
    }

    /** 
     * Procesa la respuesta de autorizacion del Engine
     */
    private void respuestaAutorizacion(String[] c) {
        Espera espera = esperando.remove(c[1]);
        if(espera == null) {
            System.out.println("Respuesta de autorizacion sin peticion pendiente: " + c[1]);
            return;
        }
        if(c[2].equals("OK")) {
            panel.fase(c[1], "AUTORIZADA");
            enCurso.put(c[1], espera.operario());
            responder(espera.operario(), "AUTORIZADO#" + c[1] + "#" + espera.duracion());
            productor.enviar(Productor.TOPIC_ORDENES, c[1], "INICIAR");
        } else {
            registro.cambiar(c[1], Estado.REGANDO, Estado.DISPONIBLE); // se libera la reserva
            denegar(espera.operario(), c[1], "NO_DISPONIBLE");
        }
    }

    /**
     * Procesa el fin de riego: libera la estacion y avisa al FO
     */
    private void finRiego(String[] c) {
        String motivo = c[5];
        enCurso.remove(c[1]);
        recuperados.remove(c[1]);
        if(motivo.equals("LIMITE_TIEMPO") || motivo.equals("PARADA_MANUAL")
                || motivo.equals("PARADA_CENTRAL") || motivo.equals("PARADA_FO")) {
            registro.cambiar(c[1], Estado.REGANDO, Estado.DISPONIBLE);
        } else if(motivo.equals("BLOQUEO_CENTRAL")) {
            registro.cambiar(c[1], Estado.REGANDO, Estado.FUERA_DE_SERVICIO);
        } // con FUGA, el estado lo pone el monitor al avisar con LEAK
        panel.terminada(c[1], litros(c[3]) + " L en " + c[4] + " s (" + motivo + ")");
        responder(c[2], "FIN_RIEGO#" + c[1] + "#" + c[3] + "#" + c[4] + "#" + motivo);
    }
    /**
     * Da formato a un volumen con un decimal; si no es un numero, lo deja como llego
     */
    private static String litros(String texto) {
        try {
            return String.format(Locale.US, "%.1f", Double.parseDouble(texto));
        } catch(NumberFormatException e) {
            return texto;
        }
    }
    /** 
     * Si el Engine no contesta en PLAZO_S, se cancela y se libera la estacion
     */
    private void vigilar(String idWS, Espera espera) {
        plazos.schedule(() -> {
            try {
                if(esperando.remove(idWS, espera)) {
                    registro.cambiar(idWS, Estado.REGANDO, Estado.DISPONIBLE);
                    productor.enviar(Productor.TOPIC_ORDENES, idWS, "CANCELAR");
                    denegar(espera.operario(), idWS, "SIN_RESPUESTA");
                }
            } catch(RuntimeException e) { // una tarea programada que falla desaparece sin avisar
                System.out.println("Error en el plazo de " + idWS + ": " + e);
            }
        }, PLAZO_S, TimeUnit.SECONDS);
    }

    /** 
     * Cada paso se muestra en la pantalla de Central y, si hay FO, se le envia
     */
    private void responder(String operario, String mensaje) {
        panel.anotar("Riego [" + operario + "] " + mensaje);
        if(!operario.equals(LOCAL)) {
            productor.enviar(Productor.TOPIC_RESPUESTAS, operario, mensaje);
        }
    }
    /**
     * Deniega un riego: se envia al FO y se muestra en la pantalla de Central
     */
    private void denegar(String operario, String idWS, String motivo) {
        panel.denegada(idWS, operario, motivo);
        responder(operario, "DENEGADO#" + idWS + "#" + motivo);
    }
}