package ua.es.engine;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.time.Duration;
import java.util.Properties;
import java.util.Scanner;
import java.util.ArrayList;
import java.util.List;

import java.io.IOException;
import java.lang.Thread;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;


public class Main {
    // Constantes
    private static final String PING = "PING";
    private static final String OK = "OK";
    private static final String KO = "KO";
    private static final int REINTENTO_MS = 3000;
    private static final int PERIODO_MS = 1000;

    // Enum para diferenciar los distintos estados de la WS
    public enum ESTADO { DISPONIBLE, PENDIENTE_INICIO, REGANDO, FUERA_SERVICIO, FUGA}

    // Estado compartido(usado por los hilos)
    private static final Object obj = new Object();

    // Declaración de productor
    private static KafkaProducer<String, String> productor;

    // Variables usadas por varios hilos
    private static volatile String idWS = null;
    private static volatile ESTADO estado = ESTADO.DISPONIBLE;
    private static volatile boolean averia = false;
    private static volatile boolean riego = false;

    // Datos del riego en curso
    private static  String idOperario = "";
    private static int duracion = 0;
    private static double volumen = 0;
    private static long inicioRiego = 0;

    // Kafka
    private static final String TOPIC_ORDENES = "topicOrdenes";       // CENTRAL → Engine
    private static final String TOPIC_EVENTOS = "topicEventos";       // Engine → CENTRAL
    private static final String TOPIC_TELEMETRIA = "topicTelemetria"; // Engine → CENTRAL

    private static void hiloMonitor(String ipMonitor, int puertoMonitor) {
        // Si se pierde el monitor, se reintenta
        for(;;) {
            try {
                try(Socket s = new Socket(ipMonitor, puertoMonitor)) {
                BufferedReader entrada = new BufferedReader(new InputStreamReader(s.getInputStream()));
                PrintWriter salida = new PrintWriter(s.getOutputStream(), true);

                System.out.println("Conectado al monitor");
                
                // Se obtiene el idWS desde el mensaje recibido
                String msg = entrada.readLine();
                if(msg != null && msg.startsWith("ASSIGN#")) {
                    idWS = msg.substring(msg.indexOf("#") + 1);
                }

                // Bucle interno para contestar a cada PING
                for(;;) {
                    msg = entrada.readLine();

                    // Se comprueba si el monitor se ha desconectado
                    if(msg == null) {
                        System.out.println("Monitor desconectado");
                        break;
                    }

                    if(msg.equals(PING)) {
                        if(averia) {
                            salida.println(KO);
                        } else {
                            salida.println(OK);
                        }
                    }
                }

                } catch(IOException e) {
                    System.out.println("Monitor no disponible, reintentando en 3 s...");
                }

            Thread.sleep(REINTENTO_MS);
            } catch(InterruptedException e) {}
        }
    }
    
    // Función auxiliar para mostrar el menú
    private static void menu() {
        Scanner sc = new Scanner(System.in);
        String entradaUsr = sc.nextLine();

        for(;;) {
            System.out.println("===== WS " + idWS + " =====");
            System.out.println("Estado: " + estado);

            // Si está regando, se muestra la información del riego
            if(estado.equals(ESTADO.REGANDO)) {
                System.out.println("Operario: " + idOperario + " Volumen: " + volumen + " L");
            }

            // Se muestran las posibles acciones
            System.out.println("Posibles acciones:");
            System.out.println("[k] -> Simular/resolver avería");
            System.out.println("[r] -> Solicitar riego");
            System.out.println("[s] -> Terminar riego");
            entradaUsr = sc.nextLine();

            // Depende de la entrada, se realiza una acción u otra
            if(entradaUsr.toLowerCase().equals("k")) {
                // De esta manera, si un hilo llega hasta este punto pero hay otro dentro,
                // el recién llegado espera la salida del actual para ejecutar el código
                synchronized(obj) {
                    // Se invierte el valor para simular la avería
                    averia = !averia;

                    // Se comprueba el nuevo valor
                    if(averia) {
                        System.out.println("Avería simulada");
                        if(estado.equals(ESTADO.REGANDO) || estado.equals(ESTADO.PENDIENTE_INICIO)) {
                            // finalizarRiego("FUGA");
                        }

                        estado = ESTADO.FUGA;
                    } else {
                        System.out.println("Averia resuelta");
                        estado = ESTADO.DISPONIBLE;
                    }
                }
            } else if(entradaUsr.toLowerCase().equals("r")) {
                System.out.println("Introduzca la duración(s): ");

                try {
                    duracion = Integer.parseInt(sc.nextLine());
                } catch(NumberFormatException e) {
                    System.out.println("Duración no válida");
                    continue; // Se vuelve al menú inicial
                }

                // Mandar solicitud de riego a la central
                productor.send(new ProducerRecord<>(TOPIC_EVENTOS, idWS, "SOLICITUD_RIEGO#" + idWS + "#LOCAL#" + duracion));
                System.out.println("Solicitud enviada a CENTRAL, esperando autorización...");
            } else if(entradaUsr.toLowerCase().equals("s")) {
                if(estado.equals(ESTADO.REGANDO)) {
                    // finalizarRiego("PARADA_MANUAL");
                } else {
                    System.out.println("No hay ningún riego en curso");
                }
            } else {
                System.out.println("Opción no válida");
            }
        }
    }

    private static void hiloKafka(String ipBroker, int puertoBroker) {
        // Se crea el consumidor local, solo utilizado por este hilo
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, ipBroker + ":" + puertoBroker);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "engine-" + idWS); // grupo propio por cada engine
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        try(KafkaConsumer<String, String> consumidor = new KafkaConsumer<>(props)) {
            // Se suscribe a los topics
            consumidor.subscribe(List.of(TOPIC_ORDENES));

            for(;;) {
                ConsumerRecords<String, String> mensajes = consumidor.poll(Duration.ofMillis(500));

                for(ConsumerRecord<String, String> msg : mensajes) {
                    // Se comprueba si la orden que acaba de llegar de Kafka es para esta estación
                    if(!idWS.equals(msg.key())) {
                        continue;
                    }

                    // Se obtienen los campos a partir del mensaje 
                    String[] campos = msg.value().split("#");
                    String accion = campos[0];

                    // Si la central pide autorización
                    if(accion.equals("AUTORIZACION")) {
                        // De esta manera, si un hilo llega hasta este punto pero hay otro dentro,
                        // el recién llegado espera la salida del actual para ejecutar el código
                        synchronized(obj) {
                            if(estado.equals(ESTADO.DISPONIBLE) && !averia) {
                                String idOperario = campos[1];

                                try {
                                    int limiteSeg = Integer.parseInt(campos[2]);
                                } catch(NumberFormatException e) {
                                    System.out.println("El número introducido no es un entero");
                                    continue;
                                }

                                estado = ESTADO.PENDIENTE_INICIO;
                                productor.send(new ProducerRecord<>(TOPIC_EVENTOS, idWS, "RESP_AUTORIZACION#" + idWS + "#OK"));
                                System.out.println("Riego autorizado para " + idOperario + ", esperando inicio");
                            } else {
                                productor.send(new ProducerRecord<>(TOPIC_EVENTOS, idWS, "RESP_AUTORIZACION#" + idWS + "#KO#" + estado));
                                System.out.println("Autorización denegada (estado " + estado + ")");
                            }
                        }
                    } else if(accion.equals("INICIAR")) { // Si la central pide iniciar
                        synchronized(obj) {
                            if(estado.equals(ESTADO.PENDIENTE_INICIO)) {
                                //iniciarRiego()
                            }
                        }
                    } else if(accion.equals("CANCELAR")) { // si la central pide cancelar
                        synchronized(obj) {
                            if(estado.equals(ESTADO.PENDIENTE_INICIO)) {
                                estado = ESTADO.DISPONIBLE;
                                System.out.println("Riego cancelado por CENTRAL");
                            }
                        }
                    } else if(accion.equals("BLOQUEAR")) { // si la central pide bloquear
                        synchronized(obj) {
                            if(estado.equals(ESTADO.REGANDO) || estado.equals(ESTADO.PENDIENTE_INICIO)) {
                            // finalizarRiego("BLOQUEO_CENTRAL")
                            }

                            estado = ESTADO.FUERA_SERVICIO;
                        } 
                        productor.send(new ProducerRecord<>(TOPIC_EVENTOS, idWS, "ESTADO#" + idWS + "#FUERA_SERVICIO"));
                        System.out.println("FUERA DE SERVICIO por orden de CENTRAL");
                    } else if(accion.equals("ACTIVAR")) { // Si la central pide activar
                        synchronized(obj) {
                            if(!averia) {
                                estado = ESTADO.DISPONIBLE;
                            }
                        }
                        productor.send(new ProducerRecord<>(TOPIC_EVENTOS, idWS, "ESTADO#" + idWS + "#" + estado));
                        System.out.println("Estado actual: " + estado);
                    } else {
                        System.out.println("Accion desconocida: " + accion);
                    }
                }
            }
        } catch(KafkaException e) {
            System.out.println("Error: No se puede conectar con Kafka");
        }
    }

    public static void main(String[] args) {
        // Se debe comprobar el número de argumentos
        if(args.length != 4) {
            System.out.println("Uso: java -jar WM_WS_E.jar <ipBroker> <puertoBroker> <ipMonitor> <puertoMonitor>");
            return;
        }

        // Se debe comprobar que los puertos introducidos sean enteros
        int puertoBroker, puertoMonitor;
        try {
            puertoBroker = Integer.parseInt(args[1]);
            puertoMonitor = Integer.parseInt(args[3]);
        } catch(NumberFormatException e) {
            System.out.println("Error: puerto no válido");
            return;
        }

        String ipBroker = args[0];
        String ipMonitor = args[2];

        // Se crea el productor
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, ipBroker + ":" + puertoBroker);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        productor = new KafkaProducer<>(props);

        // Se cierra el productor al apagar el Engine
        Runtime.getRuntime().addShutdownHook(new Thread(() -> productor.close()));

        // Se lanza el hilo que se conecta al monitor
        new Thread(() -> hiloMonitor(ipMonitor, puertoMonitor)).start();

        // Se espera a recibir el ID del monitor
        while(idWS == null) {
            try {
                Thread.sleep(100);
            } catch(InterruptedException e) {
                return;
            }
        }    

        System.out.println("Engine asignado a " + idWS);

        // Se lanza el hilo de kafka
        // new Thread(() -> hiloKafka(ipBroker, puertoBroker)).start();

        // El hilo principal se queda en el menú
        // menu()
    }
}

/*
    COSAS A DESCOMENTAR
    - final main
    - finalizarriego en menu() X2
    - iniciarRIego() en hiloKafka()
    - finalizarRiego() en ""
*/