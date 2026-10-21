package ua.es.engine;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.time.Duration;
import java.util.Properties;
import java.util.Scanner;
import java.util.List;
import java.io.IOException;


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
    private static volatile boolean bloqueada = false;

    // Datos del riego en curso
    private static  String idOperario = "";
    private static int limiteSeg = 0;
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
            } catch(InterruptedException e) {
                return;
            }
        }
    }
    
    // Función auxiliar para mostrar el menú
    private static void menu() {
        Scanner sc = new Scanner(System.in);
        String entradaUsr;

        for(;;) {
            System.out.println("===== WS " + idWS + " =====");
            System.out.println("Estado: " + estado);

            // Si está regando, se muestra la información del riego
            synchronized(obj) {
                if(estado.equals(ESTADO.REGANDO)) {
                    System.out.println("Operario: " + idOperario + " Volumen: " + volumen + " L");
                }
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
                            finalizarRiego("FUGA");
                        }

                        estado = ESTADO.FUGA;
                    } else {
                        System.out.println("Averia resuelta");
                        if(bloqueada) {
                            estado = ESTADO.FUERA_SERVICIO;
                        } else {
                            estado = ESTADO.DISPONIBLE;
                        }
                    }
                }
            } else if(entradaUsr.toLowerCase().equals("r")) {
                System.out.println("Introduzca la duración(s): ");

                int duracion;
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
                    finalizarRiego("PARADA_MANUAL");
                } else {
                    System.out.println("No hay ningún riego en curso");
                }
            } else {
                System.out.println("Opción no válida");
            }
        }
        sc.close();
    }

    // Función para gestionar el hilo de kafka
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
                                try {
                                    limiteSeg = Integer.parseInt(campos[2]);
                                    idOperario = campos[1];
                                } catch(NumberFormatException e) {
                                    System.out.println("Número no válido");
                                    productor.send(new ProducerRecord<>(TOPIC_EVENTOS, idWS, "RESP_AUTORIZACION#" + idWS + "#KO#MENSAJE_INVALIDO"));
                                    continue;
                                } catch(ArrayIndexOutOfBoundsException e) {
                                    System.out.println("Mensaje AUTORIZACION mal formado: " + msg.value());
                                    productor.send(new ProducerRecord<>(TOPIC_EVENTOS, idWS, "RESP_AUTORIZACION#" + idWS + "#KO#MENSAJE_INVALIDO"));
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
                                iniciarRiego();
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
                        String estadoActual;
                        synchronized(obj) {
                            if(estado.equals(ESTADO.REGANDO) || estado.equals(ESTADO.PENDIENTE_INICIO)) {
                                finalizarRiego("BLOQUEO_CENTRAL");
                            }

                            bloqueada = true;
                            if(!averia) {
                                estado = ESTADO.FUERA_SERVICIO;
                            }
                            estadoActual = estado.toString();
                        }
                        productor.send(new ProducerRecord<>(TOPIC_EVENTOS, idWS, "ESTADO#" + idWS + "#" + estadoActual));
                        System.out.println("Bloqueo recibido. Estado actual: " + estadoActual);
                    } else if(accion.equals("ACTIVAR")) { // Si la central pide activar
                        String estadoActual;
                        synchronized(obj) {
                            bloqueada = false;
                            if(averia) {
                                estado = ESTADO.FUGA;
                            } else if(estado.equals(ESTADO.FUERA_SERVICIO)) {
                                estado = ESTADO.DISPONIBLE;
                            }
                            estadoActual = estado.toString();
                        }
                        productor.send(new ProducerRecord<>(TOPIC_EVENTOS, idWS, "ESTADO#" + idWS + "#" + estadoActual));
                        System.out.println("Estado actual: " + estadoActual);
                    } else {
                        System.out.println("Accion desconocida: " + accion);
                    }
                }
            }
        } catch(KafkaException e) {
            System.out.println("Error: No se puede conectar con Kafka");
        }
    }

    // Función para que WS notifique a Central el fin del riego
    private static void finalizarRiego(String motivo) {
        String msg; // Será utilizado posteriormente para almacenar el mensaje que será transmitido por el productor
        
        // De esta manera, si un hilo llega hasta este punto pero hay otro dentro,
        // el recién llegado espera la salida del actual para ejecutar el código
        synchronized(obj) {
            if(!riego && !estado.equals(ESTADO.PENDIENTE_INICIO)) {
                return;
            }

            riego = false;
            System.out.println("Cerrando electroválvula (" + motivo + ")");

            long duracionReal = 0;
            if(inicioRiego != 0) { // 0 = sin empezar
                duracionReal = (System.currentTimeMillis() - inicioRiego) / 1000;
            }

            System.out.println("Resumen: " + volumen + " L en " + duracionReal + " s");
            
            inicioRiego = 0;

            if(motivo.equals("LIMITE_TIEMPO") || motivo.equals("PARADA_MANUAL")) {
                estado = ESTADO.DISPONIBLE;
            }

            msg = "FIN_RIEGO#" + idWS + "#" + idOperario + "#" + volumen + "#" + duracionReal + "#" + motivo;
        }

        // Se envia fuera para no bloquear a los demás hilos
        productor.send(new ProducerRecord<>(TOPIC_EVENTOS, idWS, msg));
    }

    // Debido a que se llama siempre ya desde synchronized(obj), aqui no hace falta ponerlo
    private static void iniciarRiego() {
        estado = ESTADO.REGANDO;
        riego = true;
        volumen = 0;
        inicioRiego = System.currentTimeMillis();

        System.out.println("Abriendo electroválvula. Riego iniciado por " + idOperario);

        // se manda la información de que el riego ha sido iniciado
        productor.send(new ProducerRecord<>(TOPIC_EVENTOS, idWS, "RIEGO_INICIADO#" + idWS + "#" + idOperario));

        // El riego va en su propio hilo para no bloquear hiloKafka
        new Thread(() -> hiloRiego()).start();
    }

    // Manda información constante(cada segundo) a central sobre el riego
    private static void hiloRiego() {
        while(riego) {
            // Se calcula el caudal
            double caudal = 8 + Math.random() * 7; // 8-15 L/min

            // Se envia fuera para no bloquear a los demás hilos
            double volumenActual;
            String operario;
            int limite;
            long segundos;

            // Se leen y se actualizan los datos compartidos con el syncrhonized(obj)
            synchronized(obj) {
                if(!riego) {
                    break;
                }

                volumen += caudal / 60; // L/min

                volumenActual = volumen;
                operario = idOperario;
                limite = limiteSeg;
                segundos = (System.currentTimeMillis() - inicioRiego) / 1000;
            }

            // Se envia fuera para no bloquear a los demás hilos
            productor.send(new ProducerRecord<>(TOPIC_TELEMETRIA, idWS, "TELEMETRIA#" + idWS + "#" + caudal + "#" + volumenActual + "#" + operario));

            if(segundos >= limite) {
                finalizarRiego("LIMITE_TIEMPO");
                break;
            }

            // Se espera al siguiente segundo
            try {
                Thread.sleep(PERIODO_MS);
            } catch(InterruptedException e) {
                return;
            }
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

        System.out.println("Engine asignado a WS " + idWS);

        // Se lanza el hilo de kafka
        new Thread(() -> hiloKafka(ipBroker, puertoBroker)).start();

        // El hilo principal se queda en el menú
        menu();
    }
}