package ua.es.central.web;

import java.util.concurrent.ConcurrentHashMap;
import java.util.Set;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

// Ordenes de la central a las WS
public class OrdenesKafka {
    private static final String TOPIC_ORDENES = "topicOrdenes";

    private final KafkaProducer<String, String> productor;
    private final EstadoPanel panel;

    // WS a las que Central ha pedido autorización y esperan respuesta
    private final Set<String> pendientesCentral = ConcurrentHashMap.newKeySet();

    public OrdenesKafka(KafkaProducer<String, String> productor, EstadoPanel panel) {
        this.productor = productor;
        this.panel = panel;
    }

    // Iniciar un riego
    public void iniciar(String idWS, int duracion) {
        pendientesCentral.add(idWS);
        enviar(idWS, "AUTORIZACION#CENTRAL#" + duracion);
        panel.anotar(idWS + " -> CENTRAL solicita un riego de " + duracion + " s");
    }

    // Bloqua la WS
    public void bloquear(String idWS) {
        enviar(idWS, "BLOQUEAR");
        panel.anotar( " -> orden de bloqueo enviada");
    }

    // Activa la WS
    public void activar(String idWS) {
        enviar(idWS, "ACTIVAR");
        panel.anotar(idWS + " -> orden de activación enviada");
    }

    // Llamar desde el consumidor de topicEventos al recibir RESP_AUTORIZACION.
    // Devuelve true si la autorización era de CENTRAL
    public boolean respuestaAutorizacion(String idWS, boolean ok, String motivo) {
        if(!pendientesCentral.remove(idWS)) {
            return false;
        }
        if(ok) {
            enviar(idWS, "INICIAR");
            panel.anotar(idWS + " → autorizado, iniciando riego de CENTRAL");
        } else {
            panel.anotar(idWS + " → riego de CENTRAL denegado (" + motivo + ")");
        }
        return true;
    }

    private void enviar(String idWS, String mensaje) {
        // La clave es el idWS: el Engine filtra por ella
        productor.send(new ProducerRecord<>(TOPIC_ORDENES, idWS, mensaje), (metadata, error) -> {
            if(error != null) {
                panel.anotar("Error: no se ha podido enviar '" + mensaje + "' a " + idWS);
            }
        });
    }
}