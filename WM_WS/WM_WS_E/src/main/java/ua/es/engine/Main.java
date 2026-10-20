package ua.es.engine;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.lang.Thread;

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
    private static int limiteSeg = 0;
    private static double volumen = 0;
    private static long inicioRiego = 0;

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