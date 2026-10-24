package ua.es.mon;

import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.SocketTimeoutException;
import java.net.Socket;
import java.io.IOException;

import ua.es.compartido.Canal;
public class Main {
    // CONSTANTES
    private static final String PING = "PING";
    private static final String OK = "OK";
    private static final String KO = "KO";
    private static final int TIMEOUT_ENGINE_MS = 2000;
    private static final int TIMEOUT_CENTRAL_MS = 3000;
    private static final int REINTENTO_MS = 3000;
    private static final int PERIODO_MS = 1000;
    private static volatile boolean averia = false; // De esta manera, el valor se mantiene entre engines
    private static final int LATIDO_MS = 2000;

    private static Canal canalCentral = null;


    // Enum para diferenciar las distintas respuestas posibles de la central
    public enum RESPUESTA {OK, KO, SIN_RESPUESTA}

    // Función para enviar información a central
    private static synchronized String enviarCentral(String ip, int puerto, String mensaje) {
        // Si no hay conexión abierta, se abre una nueva
        if(canalCentral == null) {
            Socket s = new Socket();
            try {
                s.connect(new InetSocketAddress(ip, puerto), TIMEOUT_CENTRAL_MS);

                // Se añade un tiempo de timeout
                s.setSoTimeout(TIMEOUT_CENTRAL_MS);

                Canal c = new Canal(s);
                c.abrir();
                canalCentral = c;
            } catch(IOException e) {
                try{
                    s.close();                  
                }catch(IOException ex) {}
                return null;

            }
                

            // Conexión nueva (p. ej. Central se reinició): hay que registrarse antes de enviar nada más
            if(!mensaje.startsWith("REGISTER#")) {
                String id = mensaje.split("#")[1];
                if(!OK.equals(intercambiar("REGISTER#" + id))) {
                    cerrarCentral();
                    return null;
                }
                // Al registrarse, Central pone la WS en DISPONIBLE: si hay fuga, se vuelve a avisar
                if(averia && !mensaje.startsWith("LEAK")) {
                    intercambiar("LEAK#" + id);
                }
            }
        }

        return intercambiar(mensaje);
    }

    // Cierra la conexión con Central (si la hay)
    private static synchronized void cerrarCentral() {
        if(canalCentral != null) {
            try {
                canalCentral.close();
            } catch(IOException e) {}
            canalCentral = null;
        }
    }

    // Envía un mensaje por la conexión abierta y devuelve la respuesta (null si falla)
    private static synchronized String intercambiar(String mensaje) {
        try {
            String respuesta = canalCentral.peticion(mensaje);
            if(respuesta == null) {
                cerrarCentral(); // Central ha cerrado la conexión
            }
            return respuesta;
            } catch(IOException e) {
                cerrarCentral();
                return null;
            }
        }
    

    // Función auxiliar que envia un latido periódico a Central para que sepa que está vivo
    private static void latido(String ip, int puerto, String id) {
        for(;;) {
            try{
                Thread.sleep(LATIDO_MS);
            } catch(InterruptedException e) {
                return;
            }
            if(enviarCentral(ip, puerto, "STATUS#" + id) == null) {
                System.out.println("Aviso: Central no responde.");
            }
        }
    }

    // Función para registrar al monitor en la central
    private static RESPUESTA registrar(String ip, int puerto, String id) {
        // Mensaje que espera recibir de la central
        String msg;

        // Se debe conectar a la central
        msg = enviarCentral(ip, puerto,"REGISTER#" + id);

        if(msg == null) {
            System.out.println("Error: no se puede conectar a Central");
            return RESPUESTA.SIN_RESPUESTA;
        } else if(!OK.equals(msg)) {
            System.out.println("Error: Central ha rechazado el registro");
            return RESPUESTA.KO;
        }

        return RESPUESTA.OK;
    }

    // Función para gestionar un engine cuando llega
    private static void gestionarEngine(Socket sEngine, String ipCentral, int puertoCentral, String idWS) {
        try(Socket s = sEngine; Canal canal = new Canal(s)){
            System.out.println("Engine conectado.");

            
            s.setSoTimeout(TIMEOUT_ENGINE_MS);
            canal.aceptar();
            canal.enviar("ASSIGN#" + idWS);

            // Bucle interno
            boolean fallo;
            String motivo;
            String respuesta;
            boolean desconectado;
            for(;;) {

                fallo = false;
                motivo = "";
                desconectado = false;

                try {
                    respuesta = canal.peticion(PING);
                   
                    if(respuesta == null) {
                        fallo = true;
                        motivo = "Engine desconectado";
                        desconectado = true;
                    } else if(KO.equals(respuesta)) {
                        fallo = true;
                        motivo = KO;
                    } else if(OK.equals(respuesta)) {
                        fallo = false;
                    } else {
                        fallo = true;
                        motivo = "Respuesta desconocida";
                    }
                } catch(SocketTimeoutException e) {
                    fallo = true;
                    motivo = "Sin respuesta (timeout)";
                    desconectado = true;
                } catch(IOException e) {
                    fallo = true;
                    motivo = "Engine desconectado";
                    desconectado = true;
                }

                // Solo actua cuando cambia el estado
                if(fallo && !averia) {
                    averia = true;
                    System.out.println("Avería detectada (" + motivo + ")");
                    
                    // Se debe comprobar la respuesta de la central
                    if(enviarCentral(ipCentral, puertoCentral, "LEAK#" + idWS) == null) {
                        System.out.println("Aviso: Central no responde.");
                    }

                } else if(!fallo && averia) {
                    averia = false;
                    System.out.println("Avería resuelta");
    
                    // Se debe comprobar la respuesta de la central
                    if(enviarCentral(ipCentral, puertoCentral, "LEAK_FIXED#" + idWS) == null) {
                        System.out.println("Aviso: Central no responde.");
                    }
                }

                // Si el engine se ha desconectado, salir del bucle
                if(desconectado) {
                    break;
                }

                // Se espera el tiempo del periodo
                Thread.sleep(PERIODO_MS);
            }
        } catch(IOException e) {
            System.out.println("Error con el engine");
            return;
        } catch(InterruptedException e) {
            return;
        }

        System.out.println("Engine desconectado, esperando al siguiente...");
    }

    public static void main(String[] args) {
        // Se debe comprobar si la cantidad de argumentos es la correcta
        if(args.length != 4) {
            System.out.println("Uso: java -jar WM_WS_M.jar <puertoMonitor> <ipCentral> <puertoCentral> <idWS>");
            return;
        }

        // Se debe comprobar que los puertos introducidos sean números enteros
        int puertoMonitor, puertoCentral;
        try {
            puertoMonitor = Integer.parseInt(args[0]);
            puertoCentral = Integer.parseInt(args[2]);
        } catch(NumberFormatException e) {
            System.out.println("Error: puerto no válido");
            return;
        }

        String ipCentral = args[1];
        String idWS = args[3];

        // Se abre el servidor para el engine 
        try(ServerSocket ss = new ServerSocket(puertoMonitor)) {
            RESPUESTA resp = RESPUESTA.SIN_RESPUESTA; // Se inicializa para evitar problemas de compilación
            
            try {
                // Se debe registrar en la central, para ello se conecta y espera respuesta
                while((resp = registrar(ipCentral, puertoCentral, idWS)).equals(RESPUESTA.SIN_RESPUESTA)) {
                    System.out.println("Reintentando en 3 s...");
                    Thread.sleep(REINTENTO_MS);
                }
            } catch(InterruptedException e) {
                return;
            }

            // Se debe comprobar si la central ha rechazado
            if(resp.equals(RESPUESTA.KO)) {
                return;
            }

            // Se lanza el latido para que Central sepa que el monitor sigue vivo
            new Thread(() -> latido(ipCentral, puertoCentral, idWS)).start();

            System.out.println("Monitor " + idWS + " escuchando en el puerto " + puertoMonitor);

            //Bucle para esperar engines indefinidamente
            for(;;) {
                Socket s = ss.accept();
                gestionarEngine(s, ipCentral, puertoCentral, idWS);
            }
        } catch(IOException e) {
            System.out.println("Error: no se puede abrir el puerto " + puertoMonitor);
            return;
        }
    }
}