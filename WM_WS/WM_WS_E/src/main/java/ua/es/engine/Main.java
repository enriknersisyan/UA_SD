package ua.es.engine;

import java.net.Socket;
import java.time.Duration;
import java.util.Properties;
import java.util.Scanner;
import java.util.List;
import java.util.UUID;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;


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

import ua.es.compartido.Canal;

public class Main {
    // Constantes
    private static final String PING = "PING";
    private static final String OK = "OK";
    private static final String KO = "KO";
    private static final int REINTENTO_MS = 3000;
    private static final int PERIODO_MS = 1000;

    // Enum para diferenciar los distintos estados de la WS
    public enum ESTADO { DISPONIBLE, REGANDO, FUGA, FUERA_DE_SERVICIO, DESCONECTADA }

    // Estado compartido(usado por los hilos)
    private static final Object obj = new Object();

    // Declaración de productor
    private static KafkaProducer<String, String> productor;

    // Variables usadas por varios hilos
    private static volatile String idWS = null;
    private static volatile ESTADO estado = ESTADO.DESCONECTADA;
    private static volatile boolean averia = false;
    private static volatile boolean riego = false;
    private static volatile boolean bloqueada = false;
    private static boolean pendienteInicio = false; // Riego autorizado, esperando la orden INICIAR

    // Datos del riego en curso
    private static  String idOperario = "";
    private static int limiteSeg = 0;
    private static double volumen = 0;
    private static long inicioRiego = 0;
    private static int numRiego = 0; // Para diferenciar los riegos

    // Topics Kafka
    private static final String TOPIC_ORDENES = "topicOrdenes";       // CENTRAL → Engine
    private static final String TOPIC_EVENTOS = "topicEventos";       // Engine → CENTRAL
    private static final String TOPIC_TELEMETRIA = "topicTelemetria"; // Engine → CENTRAL

    // Carpeta donde se guarda el riego en curso, para no perderlo si el Engine se cae
    // (en Docker se monta un volumen ahí)
    private static final String DIR_DATOS = System.getenv().getOrDefault("WM_DATOS_DIR", ".");

    // Archivo del riego en curso de esta estación
    private static Path archivoRiego() {
        return Path.of(DIR_DATOS, "riego_" + idWS + ".txt");
    }

    // Guarda el riego en curso: operario#volumen#inicio#ahora. Se llama dentro de synchronized(obj)
    private static void guardarRiego() {
        try {
            Files.writeString(archivoRiego(), idOperario + "#" + volumen + "#" + inicioRiego + "#" + System.currentTimeMillis());
        } catch(IOException e) {
            System.out.println("Aviso: no se puede guardar el riego en " + archivoRiego());
        }
    }

    // Borra el riego guardado al terminar. Se llama dentro de synchronized(obj)
    private static void borrarRiego() {
        try {
            Files.deleteIfExists(archivoRiego());
        } catch(IOException e) {
            System.out.println("Aviso: no se puede borrar " + archivoRiego());
        }
    }

    // Al arrancar: si quedó un riego a medias (el Engine se cayó), se envía su resumen con lo regado
    private static void recuperarRiego() {
        Path archivo = archivoRiego();
        if(!Files.exists(archivo)) {
            return;
        }
        try {
            String[] c = Files.readString(archivo).trim().split("#");
            String operario = c[0];
            double vol = Double.parseDouble(c[1]);
            long duracion = (Long.parseLong(c[3]) - Long.parseLong(c[2])) / 1000;
            System.out.println("Riego interrumpido por una caída: " + vol + " L en " + duracion + " s. Se envía el resumen a CENTRAL");
            enviar(TOPIC_EVENTOS, "FIN_RIEGO#" + idWS + "#" + operario + "#" + vol + "#" + duracion + "#AVERIA");
            Files.delete(archivo);
        } catch(IOException | RuntimeException e) {
            System.out.println("Aviso: no se puede recuperar el riego guardado en " + archivo);
        }
    }

    private static void hiloMonitor(String ipMonitor, int puertoMonitor) {
        // Si se pierde el monitor, se reintenta
        for(;;) {
            try {
                try(Socket s = new Socket(ipMonitor, puertoMonitor); Canal canal = new Canal(s)) {
                canal.abrir();

                // Se obtiene el idWS desde el mensaje recibido
                String msg = canal.recibir();
                if(msg != null && msg.startsWith("ASSIGN#")) {
                    String id = msg.substring(msg.indexOf("#") + 1);

                    // Conectada, pasa al estado que corresponda
                    synchronized(obj) {
                        if(estado.equals(ESTADO.DESCONECTADA)) {
                            estado = estadoReposo();
                        }
                    }

                    idWS = id;
                    System.out.println("Conectado al monitor");
                }

                // Bucle interno para contestar a cada PING
                for(;;) {
                    msg = canal.recibir();

                    // Se comprueba si el monitor se ha desconectado
                    if(msg == null) {
                        System.out.println("Monitor desconectado");
                        break;
                    }

                    if(msg.equals(PING)) {
                        if(averia) {
                            canal.enviar(KO);
                        } else {
                            canal.enviar(OK);
                        }
                    }
                }

                } catch(IOException e) {
                    System.out.println("Monitor no disponible, reintentando en 3 s...");
                }
            
                // Tanto si el monitor se ha cerrado como si no se ha podido conectar
                desconectarMonitor();

                Thread.sleep(REINTENTO_MS);
            } catch(InterruptedException e) {
                return;
            }
        }
    }

    // Estado de la WS cuando no está regando (se llama siempre dentro de synchronized(obj))
    private static ESTADO estadoReposo() {
        if(averia) {
            return ESTADO.FUGA;
        }

        if(bloqueada) {
            return ESTADO.FUERA_DE_SERVICIO;
        }

        return ESTADO.DISPONIBLE;
    }

    // La WS deja de estar conectada al sistema central a través del monitor
    private static void desconectarMonitor() {
        synchronized(obj) {
            if(estado.equals(ESTADO.DESCONECTADA)) {
                return;
            }

            // Sin monitor no se puede supervisar el riego, se detiene
            if(riego || pendienteInicio) {
                finalizarRiego("DESCONEXION");
            }

            estado = ESTADO.DESCONECTADA;
            System.out.println("WS desconectada del monitor");
        }
    }
    
    // Función auxiliar para mostrar el menú
    private static void menu() {
        Scanner sc = new Scanner(System.in);
        String entradaUsr;

        try {
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
                            if(riego || pendienteInicio) {
                                finalizarRiego("FUGA");
                            }
                        } else {
                            System.out.println("Averia resuelta");
                        }

                        // FUGA si hay avería; si no, FUERA_SERVICIO si está bloqueada o DISPONIBLE
                        if(!estado.equals(ESTADO.DESCONECTADA)) {
                            estado = estadoReposo();
                        }
                    }
                } else if(entradaUsr.toLowerCase().equals("r")) {
                    // Sin Monitor la estación no está supervisada: no admite riegos
                    if(estado.equals(ESTADO.DESCONECTADA)) {
                        System.out.println("WS AVERIADA: sin conexión con el Monitor. No se admiten riegos");
                        continue;
                    }
                    System.out.println("Introduzca la duración(s): ");

                    int duracion;
                    try {
                        duracion = Integer.parseInt(sc.nextLine());
                    } catch(NumberFormatException e) {
                        System.out.println("Duración no válida");
                        continue; // Se vuelve al menú inicial
                    }

                    // Mandar solicitud de riego a la central
                    enviar(TOPIC_EVENTOS, "SOLICITUD_RIEGO#" + idWS + "#LOCAL#" + duracion);
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
        } finally {
            sc.close();
        } 
    }

    // Envía un mensaje a Kafka y avisa si no llega al broker (equivale a delivery_report)
    private static void enviar(String topic, String mensaje) {
        productor.send(new ProducerRecord<>(topic, idWS, mensaje), (metadata, error) -> {
            if(error != null) {
                System.out.println("Error: no se ha podido enviar a Kafka '" + mensaje + "': " + error.getMessage());
            }
        });
    }

    // Función para gestionar el hilo de kafka
    private static void hiloKafka(String ipBroker, int puertoBroker) {
        // Se crea el consumidor local, solo utilizado por este hilo
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, ipBroker + ":" + puertoBroker);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "engine-" + idWS + "-" + UUID.randomUUID()); // grupo nuevo en cada arranque
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

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
                            if(estado.equals(ESTADO.DISPONIBLE) && !averia && !pendienteInicio) {
                                try {
                                    limiteSeg = Integer.parseInt(campos[2]);
                                    idOperario = campos[1];
                                } catch(NumberFormatException e) {
                                    System.out.println("Número no válido");
                                    enviar(TOPIC_EVENTOS, "RESP_AUTORIZACION#" + idWS + "#KO#MENSAJE_INVALIDO");
                                    continue;
                                } catch(ArrayIndexOutOfBoundsException e) {
                                    System.out.println("Mensaje AUTORIZACION mal formado: " + msg.value());
                                    enviar(TOPIC_EVENTOS, "RESP_AUTORIZACION#" + idWS + "#KO#MENSAJE_INVALIDO");
                                    continue;
                                }

                                pendienteInicio = true;
                                enviar(TOPIC_EVENTOS, "RESP_AUTORIZACION#" + idWS + "#OK");
                                System.out.println("Riego autorizado para " + idOperario + ", esperando inicio");
                            } else {
                                // Si ya hay un riego autorizado pendiente, la WS está ocupada
                                String motivo = pendienteInicio ? "OCUPADA" : estado.toString();
                                enviar(TOPIC_EVENTOS, "RESP_AUTORIZACION#" + idWS + "#KO#" + motivo);
                                System.out.println("Autorización denegada (" + motivo + ")");
                            }
                        }
                    } else if(accion.equals("INICIAR")) { // Si la central pide iniciar
                        synchronized(obj) {
                            if(pendienteInicio && estado.equals(ESTADO.DISPONIBLE)) {
                                iniciarRiego();
                            }
                        }
                    } else if(accion.equals("CANCELAR")) { // si la central pide cancelar
                        synchronized(obj) {
                            if(pendienteInicio) {
                                pendienteInicio = false;
                                System.out.println("Riego cancelado por CENTRAL");
                            }
                        }
                    } else if(accion.equals("BLOQUEAR")) { // si la central pide bloquear
                        String estadoActual;
                        synchronized(obj) {
                            bloqueada = true;
                            if(riego || pendienteInicio) {
                                finalizarRiego("BLOQUEO_CENTRAL");
                            }
                            
                            // FUERA_SERVICIO, o FUGA si hay avería (la fuga tiene prioridad)
                            if(!estado.equals(ESTADO.DESCONECTADA)) {
                                estado = estadoReposo();
                            }
                            estadoActual = estado.toString();
                        }
                        enviar(TOPIC_EVENTOS, "ESTADO#" + idWS + "#" + estadoActual);
                        System.out.println("Bloqueo recibido. Estado actual: " + estadoActual);
                    } else if(accion.equals("ACTIVAR")) { // Si la central pide activar
                        String estadoActual;
                        synchronized(obj) {
                            bloqueada = false;

                            // Un riego en marcha no se interrumpe al activar
                            if(!estado.equals(ESTADO.DESCONECTADA) && !estado.equals(ESTADO.REGANDO)) {
                                estado = estadoReposo();
                            }

                            estadoActual = estado.toString();
                        }
                        enviar(TOPIC_EVENTOS, "ESTADO#" + idWS + "#" + estadoActual);
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
            if(!riego && !pendienteInicio) {
                return;
            }

            riego = false;
            pendienteInicio = false;
            System.out.println("Cerrando electroválvula (" + motivo + ")");

            long duracionReal = 0;
            double volumenFinal = 0;
            if(inicioRiego != 0) { // 0 = sin empezar
                duracionReal = (System.currentTimeMillis() - inicioRiego) / 1000;
                volumenFinal = volumen;
            }

            System.out.println("Resumen: " + volumenFinal + " L en " + duracionReal + " s");

            inicioRiego = 0;
            borrarRiego(); // terminado: ya no hay nada que recuperar

            if(!estado.equals(ESTADO.DESCONECTADA)) {
                estado = estadoReposo();
            }

            msg = "FIN_RIEGO#" + idWS + "#" + idOperario + "#" + volumenFinal + "#" + duracionReal + "#" + motivo;
        }

        // Se envia fuera para no bloquear a los demás hilos
        enviar(TOPIC_EVENTOS, msg);
    }

    // Debido a que se llama siempre ya desde synchronized(obj), aqui no hace falta ponerlo
    private static void iniciarRiego() {
        pendienteInicio = false;
        estado = ESTADO.REGANDO;
        riego = true;
        volumen = 0;
        inicioRiego = System.currentTimeMillis();
        numRiego++;
        int r = numRiego;
        guardarRiego();

        System.out.println("Abriendo electroválvula. Riego iniciado por " + idOperario);

        // se manda la información de que el riego ha sido iniciado
        enviar(TOPIC_EVENTOS, "RIEGO_INICIADO#" + idWS + "#" + idOperario);

        // El riego va en su propio hilo para no bloquear hiloKafka
        new Thread(() -> hiloRiego(r)).start();
    }

    // Manda información constante(cada segundo) a central sobre el riego
    private static void hiloRiego(int r) {
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
                if(!riego || r != numRiego) {
                    break;
                }

                volumen += caudal / 60; // L/min
                guardarRiego(); // dentro del lock: así no se puede guardar un riego que ya terminó

                volumenActual = volumen;
                operario = idOperario;
                limite = limiteSeg;
                segundos = (System.currentTimeMillis() - inicioRiego) / 1000;
            }

            // Se envia fuera para no bloquear a los demás hilos
            enviar(TOPIC_TELEMETRIA, "TELEMETRIA#" + idWS + "#" + caudal + "#" + volumenActual + "#" + operario);

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

        // Si el Engine se cayó en mitad de un riego, se envía ahora lo que se regó
        recuperarRiego();

        // Se lanza el hilo de kafka
        new Thread(() -> hiloKafka(ipBroker, puertoBroker)).start();

        // El hilo principal se queda en el menú
        menu();
    }
}