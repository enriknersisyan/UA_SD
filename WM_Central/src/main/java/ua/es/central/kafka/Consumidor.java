package ua.es.central.kafka;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import ua.es.central.logica.Riegos;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.serialization.StringDeserializer;

// Un unico hilo lee los tres topics de entrada. KafkaConsumer no es seguro entre hilos,
// asi que solo este hilo lo usa.
public class Consumidor implements Runnable {
    public static final String TOPIC_PETICIONES = "topicPeticiones";  // FO -> Central
    public static final String TOPIC_EVENTOS = "topicEventos";        // Engine -> Central
    public static final String TOPIC_TELEMETRIA = "topicTelemetria";  // Engine -> Central

    private final String broker;
    private final Riegos riegos;

    public Consumidor(String broker, Riegos riegos) {
        this.broker = broker;
        this.riegos = riegos;
    }
    /**
     * Arranca el consumidor de Kafka en un hilo aparte
     */
    @Override
    public void run() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, broker);
        // Grupo fijo: Kafka recuerda por donde iba leyendo Central, asi que al reiniciarse procesa
        // lo que llego mientras estaba caida (no se pierde ninguna transaccion). "latest" solo
        // se usa la primera vez, cuando el grupo aun no tiene nada guardado.
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "central");
        // Miembro estatico: al reiniciarse tras una caida, ocupa su sitio al momento en vez de
        // esperar a que Kafka de por muerta a la instancia anterior (unos 45 s)
        props.put(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, "central-1");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        try(KafkaConsumer<String, String> consumidor = new KafkaConsumer<>(props)) {
            consumidor.subscribe(List.of(TOPIC_PETICIONES, TOPIC_EVENTOS, TOPIC_TELEMETRIA));

            for(;;) {
                ConsumerRecords<String, String> mensajes = consumidor.poll(Duration.ofMillis(500));
                for(ConsumerRecord<String, String> msg : mensajes) {
                    try {
                        procesar(msg.topic(), msg.value());
                    } catch(RuntimeException e) { // un mensaje roto no debe parar el consumidor
                        System.out.println("Error procesando '" + msg.value() + "': " + e);
                    }
                }
            }
        } catch(KafkaException e) {
            System.out.println("Error de Kafka: " + e.getMessage());
        }
    }

    // Aqui llega cada mensaje. Los pasos siguientes sustituyen el println por la logica de cada topic.
    private void procesar(String topic, String valor) {
        if(topic.equals(TOPIC_PETICIONES)) {
            riegos.peticion(valor);
        } else if(topic.equals(TOPIC_EVENTOS)) {
            riegos.evento(valor);
        } else {
            riegos.telemetria(valor);
        }
    }
}