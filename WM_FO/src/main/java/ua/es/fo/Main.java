package ua.es.fo;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Scanner;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
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

    // Central publica el estado de las estaciones cada 2 s. Si deja de llegar, Central ha caído.
    private static final int SIN_CENTRAL_MS = 6000;
    private static volatile long ultimoEstado = System.currentTimeMillis(); // ms del último ESTADO recibido

    // Recibe mensajes mientras el operario usa el menú
    private static void hiloConsumidor(String ipBroker, int puertoBroker) {
        // Consumidor local
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, ipBroker + ":" + puertoBroker);
        // Grupo fijo por operario: si el FO se cae, al volver recibe lo que le llegó mientras estaba fuera
        // (por ejemplo, el resumen de su riego). Miembro estático: al reiniciarse ocupa su sitio al momento.
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "fo-" + idOperario);
        props.put(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, "fo-" + idOperario);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest"); // solo la primera vez que se usa este ID

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
        if(campos.length != 4 || !campos[0].equals("ESTADO")) {
            System.out.println("Mensaje de estado mal formado: " + valor);
            return;
        }

        String idWS = campos[1];
        String ubi = campos[2];
        String nuevo = campos[3];
        ultimoEstado = System.currentTimeMillis(); // Central sigue viva

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
                // No es la solicitud en curso: puede ser un riego anterior (por ejemplo, el FO se cayó
                // y al volver recibe lo que se perdió). El resultado y las averías se muestran igual.
                if(tipo.equals("FIN_RIEGO")) {
                    System.out.println("===== Resultado de un riego anterior en " + idWS + " =====");
                    System.out.println("Volumen total: " + campos[2] + " L, duración: " + campos[3] + " s, motivo: " + campos[4]);
                } else if(tipo.equals("AVERIA")) {
                    System.out.println("*** Avería en " + idWS + " durante un riego anterior (" + campos[2] + ") ***");
                }
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
                System.out.println("Motivo: " + campos[4]);

                if(campos[4].equals("FUGA")) {
                    System.out.println("Riego interrumpido por FUGA");
                } else if(campos[4].equals("BLOQUEO_CENTRAL")) {
                    System.out.println("Riego detenido por CENTRAL");
                }

                concluida = true;
                resultado = "EXITO";
                obj.notifyAll();
            } else if(tipo.equals("AVERIA")) {
                System.out.println("*** AVERÍA en " + idWS + " (" + campos[2] + "): el riego se ha interrumpido ***");
                System.out.println("El resumen llegará cuando la estación se recupere");
                concluida = true;
                resultado = "FRACASO";
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

    // Muestra todas las estaciones conocidas y su estado 
    private static void mostrarEstaciones() {
        Map<String, String[]> copia;

        // Se copia la tabla con el lock y se muestra fuera, para no bloquear al consumidor
        synchronized(obj) {
            copia = new TreeMap<>(estaciones); // TreeMap: ordenada por idWS
        }

        if(copia.isEmpty()) {
            System.out.println("Todavía no se ha recibido el estado de ninguna estación");
            return;
        }

        System.out.println(String.format("%-8s | %-20s | %s", "ID", "Ubicación", "Estado"));
        System.out.println("---------+----------------------+----------------");

        for(Map.Entry<String, String[]> e : copia.entrySet()) {
            String idWS = e.getKey();
            String ubicacion = e.getValue()[0];
            String estado = e.getValue()[1];

            String marca = estado.equals("DISPONIBLE") ? "  <-- disponible" : "";
            System.out.println(String.format("%-8s | %-20s | %s%s", idWS, ubicacion, estado, marca));
        }
    }

        // Solicita un riego de forma puntual en una WS (Field Operators, pág. 11)
    private static void solicitudManual(Scanner sc) {
        // Solo la comprobación va con el lock; leer del teclado NUNCA con el lock cogido
        synchronized(obj) {
            if(solicitudEnCurso && !concluida) {
                System.out.println("Ya tienes una solicitud en curso en " + wsSolicitud);
                return;
            }
        }

        System.out.println("ID de la estación: ");
        String idWS = sc.nextLine().trim();

        System.out.println("Duración (s): ");
        int duracion;
        try {
            duracion = Integer.parseInt(sc.nextLine().trim());
        } catch(NumberFormatException e) {
            System.out.println("Duración no válida");
            return;
        }
        if(duracion <= 0) {
            System.out.println("Duración no válida");
            return;
        }

        // Aviso orientativo; quien decide es CENTRAL (pág. 5)
        String[] info;
        synchronized(obj) {
            info = estaciones.get(idWS);
        }
        if(info == null) {
            System.out.println("Aviso: no se conoce la estación " + idWS);
        } else if(!info[1].equals("DISPONIBLE")) {
            System.out.println("Aviso: " + idWS + " aparece como " + info[1]);
        }

        if(!enviarSolicitud(idWS, duracion)) {
            System.out.println("Ya tienes una solicitud en curso");
        }
        // No se espera: el hilo consumidor irá mostrando las respuestas
    }

    private static boolean enviarSolicitud(String idWS, int duracion) {
        synchronized(obj) {
            // Comprobar y marcar en el mismo bloque, evita dos solicitudes a la vez
            if(solicitudEnCurso && !concluida) {
                return false;
            }

            solicitudEnCurso = true;
            wsSolicitud = idWS;
            concluida = false;
            resultado = null;
            ultimoMensaje = System.currentTimeMillis();
        }

        productor.send(new ProducerRecord<>(TOPIC_PETICIONES, idOperario, "SOLICITUD_RIEGO#" + idOperario + "#" + idWS + "#" + duracion),
        (metadata, error) -> {
            if(error != null) {
                System.out.println("Error: la solicitud no ha llegado a Kafka: " + error.getMessage());
            }
        });
        System.out.println("Solicitud de riego enviada a CENTRAL para " + idWS + " (" + duracion + " s)");
        return true;
    }

    private static void hiloArchivo(String ruta) {
        List<String> lineas;

        try {
            lineas = Files.readAllLines(Path.of(ruta));
        } catch(IOException e) {
            System.out.println("No se puede abrir el archivo " + ruta);
            archivoEnProceso = false;
            return;
        }

        System.out.println("Se han cargado " + lineas.size() + " líneas");
        
        int completadas = 0;
        int denegadas = 0;
        int sinRespuesta = 0;

        for(int i = 0; i < lineas.size(); i++) {
            String linea = lineas.get(i).trim();

            // Se comprueba si la linea está vacia o empieza por #
            if(linea.isEmpty() || linea.startsWith("#")) {
                continue;
            }

            String[] campos = linea.split("#");
            String idWS;
            int duracion;
            try {
                idWS = campos[0].trim();
                duracion = Integer.parseInt(campos[1].trim());
            } catch(NumberFormatException | ArrayIndexOutOfBoundsException e) {
                System.out.println("Línea " + (i + 1) + " mal formada, se omite");
                continue;
            }
            if(idWS.isEmpty() || duracion <= 0) {
                System.out.println("Línea " + (i + 1) + " mal formada, se omite");
                continue;
            }

            // Si hay una solicitud manual en curso, se espera a que termine
            try {
                while(!enviarSolicitud(idWS, duracion)) {
                    Thread.sleep(1000);
                }
            } catch(InterruptedException e) {
                archivoEnProceso = false;
                return;
            }

            System.out.println("Solicitud de la línea " + (i + 1) + ": " + idWS + " durante " + duracion + " s");

            String res = esperarConclusion();

            if(res.equals("EXITO")) {
                completadas++;
                System.out.println("Línea " + (i + 1) + ": completada");
            } else if(res.equals("FRACASO")) {
                denegadas++;
                System.out.println("Línea " + (i + 1) + ": denegada");
            } else {
                sinRespuesta++;
                System.out.println("Línea " + (i + 1) + ": sin respuesta de CENTRAL");
            }

            synchronized(obj) {
                solicitudEnCurso = false;
            }

            // Espera de 4 s antes de la siguiente
            if(i < lineas.size() - 1) {
                System.out.println("Esperando 4 segundos...");
                try {
                    Thread.sleep(ESPERA_MS);
                } catch(InterruptedException e) {
                    archivoEnProceso = false;
                    return;
                }
            }
        }

        System.out.println("Archivo terminado: " + completadas + " completadas, " + denegadas + " denegadas, " + sinRespuesta + " sin respuesta");
        archivoEnProceso = false;
    }
    
    private static String esperarConclusion() {
        // Espera hasta FIN_RIEGO, DENEGADO, o demasiado tiempo sin noticias
        synchronized(obj) {
            while(!concluida) {
                long pasado = System.currentTimeMillis() - ultimoMensaje;

                if(pasado >= MAX_ESPERA_MS) {
                    System.out.println("CENTRAL no responde para " + wsSolicitud);
                    concluida = true;
                    resultado = "SIN_RESPUESTA";
                    break;
                }

                try {
                    obj.wait(1000);
                } catch(InterruptedException e) {
                    return "SIN_RESPUESTA";
                }
            }

            return resultado;
        }
    }

    // Avisa si Central deja de dar señales de vida (no llegan estados) y cuando vuelve
    private static void hiloVigilante() {
        boolean centralViva = true;
        for(;;) {
            try {
                Thread.sleep(2000);
            } catch(InterruptedException e) {
                return;
            }
            boolean ahora = System.currentTimeMillis() - ultimoEstado < SIN_CENTRAL_MS;
            if(centralViva && !ahora) {
                System.out.println("*** Imposible conectar con WM_Central: no llegan noticias suyas. No se pueden pedir riegos ***");
            } else if(!centralViva && ahora) {
                System.out.println("*** Conexión con WM_Central restablecida ***");
            }
            centralViva = ahora;
        }
    }

    // Menú del operario (hilo principal)
    private static void menu() {
        Scanner sc = new Scanner(System.in); // una sola vez, sin cerrar (cerraría System.in)

        for(;;) {
            System.out.println("===== FO " + idOperario + " =====");

            // Si hay una solicitud en curso, se muestra
            synchronized(obj) {
                if(solicitudEnCurso && !concluida) {
                    System.out.println("Solicitud en curso: " + wsSolicitud);
                }
            }

            // Se muestran las posibles acciones
            System.out.println("[1] -> Ver estaciones disponibles");
            System.out.println("[2] -> Solicitar riego");
            System.out.println("[3] -> Cargar solicitudes desde archivo");
            System.out.println("[4] -> Salir");

            String opcion = sc.nextLine().trim();

            if(opcion.equals("1")) {
                mostrarEstaciones();
            } else if(opcion.equals("2")) {
                solicitudManual(sc);
            } else if(opcion.equals("3")) {
                System.out.println("Ruta del archivo: ");
                String ruta = sc.nextLine().trim();

                if(archivoEnProceso) {
                    System.out.println("Ya se está procesando un archivo");
                } else {
                    archivoEnProceso = true;
                    new Thread(() -> hiloArchivo(ruta)).start(); // el menú no espera
                }
            } else if(opcion.equals("4")) {
                System.out.println("Saliendo");
                System.exit(0); // termina también los demás hilos y cierra el productor
            } else {
                System.out.println("Opción no válida");
            }
        }
    }

        public static void main(String[] args) {
        // Se debe comprobar el número de argumentos
        if(args.length < 3 || args.length > 4) {
            System.out.println("Uso: java -jar WM_FO.jar <ipBroker> <puertoBroker> <idOperario> [archivo]");
            return;
        }

        // Se debe comprobar que el puerto introducido sea un entero
        int puertoBroker;
        try {
            puertoBroker = Integer.parseInt(args[1]);
        } catch(NumberFormatException e) {
            System.out.println("Error: puerto no válido");
            return;
        }

        String ipBroker = args[0];
        idOperario = args[2];

        // Se crea el productor
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, ipBroker + ":" + puertoBroker);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        productor = new KafkaProducer<>(props);

        // Se cierra el productor al apagar el FO
        Runtime.getRuntime().addShutdownHook(new Thread(() -> productor.close()));

        // Se lanza el hilo que recibe todo lo que llega de CENTRAL
        new Thread(() -> hiloConsumidor(ipBroker, puertoBroker)).start();

        // Se lanza el hilo que avisa si Central cae
        new Thread(Main::hiloVigilante).start();

        // Se espera un momento para que lleguen los primeros estados
        try {
            Thread.sleep(1500);
        } catch(InterruptedException e) {
            return;
        }

        System.out.println("FO " + idOperario + " conectado");

        // Archivo opcional pasado como argumento
        if(args.length == 4) {
            archivoEnProceso = true;
            String ruta = args[3];
            new Thread(() -> hiloArchivo(ruta)).start();
        }

        // El hilo principal se queda en el menú
        menu();
    }
}