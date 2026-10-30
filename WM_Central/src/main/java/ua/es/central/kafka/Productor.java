package ua.es.central.kafka;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.function.Consumer;

// Un unico productor para todo Central. KafkaProducer es seguro entre hilos.
public class Productor {
    public static final String TOPIC_ORDENES = "topicOrdenes";       // Central -> Engine
    public static final String TOPIC_RESPUESTAS = "topicRespuestas"; // Central -> FO
    public static final String TOPIC_ESTADOS = "topicEstados";       // Central -> todos los FO

    private final KafkaProducer<String, String> productor;
    private Consumer<Boolean> oyente; // se le dice true o false segun el resultado de cada envio
    
    
    /**
     * Crea un productor de Kafka con el broker indicado
     * @param broker broker de Kafka (host:puerto)
     */
    public Productor(String broker) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, broker);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // Sin broker, send() bloquea hasta este plazo (por defecto 60 s): se acorta para no congelar a quien envia
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 5000);
        productor = new KafkaProducer<>(props);
    }


    /**
     * Envia un mensaje a un topic de Kafka
     * @param topic topic al que enviar
     * @param clave clave del mensaje
     * @param valor valor del mensaje
     */
    public void enviar(String topic, String clave, String valor) {
        productor.send(new ProducerRecord<>(topic, clave, valor), (metadatos, error) -> {
            if(error != null) {
                System.out.println("Error enviando a " + topic + ": " + error.getMessage());
            }
            if(oyente != null) {
                oyente.accept(error == null);
            }
        });
    }
    /**
     * Registra quien quiere saber si el ultimo envio a Kafka funciono (por ejemplo, el panel)
     */
    public void alCambiarConexion(Consumer<Boolean> oyente) {
        this.oyente = oyente;
    }

    // Envia lo que quede pendiente y cierra
    public void close() {
        productor.close();
    }
}