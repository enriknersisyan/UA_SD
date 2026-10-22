package ua.es.fo;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.time.Duration;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class Main {
    // Constantes
    private static final int ESPERA_MS = 4000;
    private static final int MAX_ESPERA_MS = 10000;

    // Topics Kafka
    private static final String TOPIC_PETICIONES = "topicPeticiones";
    private static final String TOPIC_RESPUESTAS = "topicRespuestas";
    private static final String TOPIC_ESTADOS = "topicEstados";

    // Datos fijos tras arrancar
    private static String idOperario;
    private static KafkaProducer<String, String> productor;

    // Datos compartidos entre hilos
    private static final Object obj = new Object();
    private static final Map<String, String[]> estaciones = new HashMap<>(); // idWS → {ubicacion, estado}
    private static boolean solicitudEnCurso = false;
    private static String wsSolicitud = null;
    private static boolean concluida = false;
    private static String resultado = null;      // "EXITO", "FRACASO" o "SIN_RESPUESTA"
    private static long ultimoMensaje = 0;       // ms del último mensaje sobre la solicitud


    private static volatile boolean archivoEnProceso = false; // Para no lanzar dos archivos a la vez

    // Recibe mensajes mientras el operario usa el menú
    private static void hiloConsumidor(String ipBroker, int puertoBroker) {
        // Consumidor local
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, ipBroker + ":" + puertoBroker);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "fo-" + idOperario);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        try(KafkaConsumer<String, String> consumidor = new KafkaConsumer<>(props)) {
            // Se suscribe a los topics
            consumidor.subscribe(List.of(TOPIC_RESPUESTAS, TOPIC_ESTADOS));

            for(;;) {
                ConsumerRecords<String, String> mensajes = consumidor.poll(Duration.ofMillis(500));

                for(ConsumerRecord<String, String> msg : mensajes) {
                    try {
                        if(msg.topic().equals(TOPIC_ESTADOS)) {
                            procesarEstado(msg.value());
                        } else if(msg.topic().equals(TOPIC_RESPUESTAS)) {
                            if(!idOperario.equals(msg.key())) {
                                continue;
                            }
                            procesarRespuesta(msg.value());
                        }
                    } catch(NumberFormatException | ArrayIndexOutOfBoundsException e) {
                        System.out.println("Mensaje mal formado: " + msg.value());
                    }
                }
            }
        } catch(KafkaException e) {
            System.out.println("Error: no se puede conectar con Kafka");
        }
    }

    private static void procesarEstado(String valor) {
        String[] campos = valor.split("#");

        // Comprobar el formato del mensaje
        if(campos.length != 4 || campos[0] != "ESTADO") {
            System.out.println("Mensaje de estado mal formado: " + valor);
            return;
        }

        String idWS = campos[1];
        String ubi = campos[2];
        String nuevo = campos[3];

        synchronized(obj) {
            estaciones.put(idWS, new String[]{ubi, nuevo});

            // Solo avisa si la WS de la solicitud entra en fuga
            if(solicitudEnCurso && !concluida && idWS.equals(wsSolicitud) && nuevo.equals("FUGA")) {
                System.out.println("*** ATENCIÓN: Fuga detectada en " + idWS + " ***");
            }
        }
    }

    private static void procesarRespuesta(String valor) {
        String[] campos = valor.split("#");
        String tipo = campos[0];
        String idWS = campos[1];

        synchronized(obj) {
            if(!solicitudEnCurso || concluida || !idWS.equals(wsSolicitud)) {
                System.out.println("Mensaje de otra estación ignorado: " + valor);
                return;
            }

            ultimoMensaje = System.currentTimeMillis();

            if(tipo.equals("RECIBIDA")) {
                System.out.println("CENTRAL ha recibido la petición para " + idWS + " y la está comprobando");
            } else if(tipo.equals("DENEGADO")) {
                String motivo = traducirMotivo(campos[2]);
                System.out.println("*** Riego DENEGADO en " + idWS + ": " + motivo + " ***");
                concluida = true;
                resultado = "FRACASO";
                obj.notifyAll();
            } else if(tipo.equals("AUTORIZADO")) {
                System.out.println("Riego AUTORIZADO en " + idWS + " durante " + campos[2] + " s");
            } else if(tipo.equals("TELEMETRIA")) {
                System.out.println(idWS + " regando: caudal " + campos[2] + " L/min, volumen " + campos[3] + " L");
            } else if(tipo.equals("FIN_RIEGO")) {
                System.out.println("===== FIN DEL RIEGO en " + idWS + " =====");
                System.out.println("Volumen total: " + campos[2] + " L");
                System.out.println("Duración: " + campos[3] + " s");
                System.err.println("Motivo: " + campos[4]);

                if(campos[4].equals("FUGA")) {
                    System.out.println("Riego interrumpido por FUGA");
                } else if(campos[4].equals("BLOQUEO_CENTRAL")) {
                    System.out.println("Riego detenido por CENTRAL");
                }

                concluida = true;
                resultado = "EXITO";
                obj.notifyAll();
            } else {
                System.out.println("Mensaje desconocido: " + valor);
            }
        }
    }

    // Traduce el motivo de rechazo de CENTRAL a un texto claro para el operario
    private static String traducirMotivo(String codigo) {
        switch(codigo) {
            case "NO_DISPONIBLE":
                return "la estación no está disponible";
            case "FUGA":
                return "la estación tiene una fuga";
            case "OPERARIO_DESCONOCIDO":
                return "tu ID no está dado de alta en CENTRAL";
            case "SIN_RESPUESTA":
                return "la estación no responde";
            default:
                return codigo; // motivo desconocido: se muestra tal cual
        }
    }

    public static void main(String[] args) {
        // Se debe comprobar el número de argumentos
        if(args.length < 3 || args.length > 4) {
            System.out.println("Uso: java -jar WM_FO.jar <ipBroker> <puertoBroker> <idOperario> [archivo]");
            return;
        }
    }
}