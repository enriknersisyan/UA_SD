package ua.es.mon;

import java.net.ServerSocket;
import java.net.Socket;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.SocketTimeoutException;
import java.lang.Thread;

public class Main {
    // CONSTANTES
    private static String PING = "PING";
    private static String OK = "OK";
    private static String KO = "KO";
    private static int TIMEOUT_ENGINE_MS = 2000;
    private static int TIMEOUT_CENTRAL_MS = 3000;
    private static int PERIODO_MS = 1000;

    // Función para enviar información a central
    private static String enviarCentral(String ip, int puerto, String mensaje) {
        try(Socket s = new Socket(ip, puerto)) {
            BufferedReader entrada = new BufferedReader(new InputStreamReader(s.getInputStream()));
            PrintWriter salida = new PrintWriter(s.getOutputStream(), true);

            // Se añade un tiempo de timeout
            s.setSoTimeout(TIMEOUT_CENTRAL_MS);
            salida.println(mensaje);
            return entrada.readLine();
        } catch(SocketTimeoutException e) {     
            return null;
        } catch(IOException e) {
            return null;
        }
    }

    // Función para registrar al monitor en la central
    private static boolean registrar(String ip, int puerto, String id) {
        // Mensaje que espera recibir de la central
        String msg;

        // Se debe conectar a la central
        msg = enviarCentral(ip, puerto,"REGISTER#" + id);

        if(msg == null) {
            System.out.println("Error: no se puede conectar a la central");
            return false;
        } else if(msg != "OK") {
            System.out.println("Error: Central ha rechazado el registro");
            return false;
        }

        return true;
    }

    // Función para gestionar un engine cuando llega
    private static void gestionarEngine(Socket sEngine, String ipCentral, int puertoCentral, String idWS) {
        // Cada engine empieza sin fallos
        boolean averia = false;

        try(Socket s = sEngine) {
            System.out.println("Engine conectado.");

            BufferedReader entrada = new BufferedReader(new InputStreamReader(s.getInputStream()));
            PrintWriter salida = new PrintWriter(s.getOutputStream(), true);

            salida.println("ASSIGN#" + idWS);
            s.setSoTimeout(TIMEOUT_ENGINE_MS);

            // Bucle interno
            boolean fallo;
            String motivo;
            String respuesta;
            boolean desconectado;
            for(;;) {
                salida.println(PING);

                fallo = false;
                motivo = "";
                desconectado = false;

                try {
                    respuesta = entrada.readLine();
                   
                    if(respuesta == null) {
                        fallo = true;
                        motivo = "Engine desconectado";
                        desconectado = true;
                    } else if(respuesta == KO) {
                        fallo = true;
                        motivo = KO;
                    } else if(respuesta == OK) {
                        fallo = false;
                    } else {
                        fallo = true;
                        motivo = "Respuesta desconocida";
                    }
                } catch(SocketTimeoutException e) {
                    fallo = true;
                    motivo = "Sin respuesta(timeout)";
                } catch(IOException e) {
                    fallo = true;
                    motivo = "Engine desconectado";
                    desconectado = true;
                }

                // Solo actua cuando cambia el estado
                if(fallo && !averia) {
                    averia = true;
                    System.out.println("Averia detectada (" + motivo + ")");
                    enviarCentral(ipCentral, puertoCentral, "LEAK#" + idWS);
                } else if(fallo && averia) {
                    averia = false;
                    System.out.println("Averia resuelta");
                    enviarCentral(ipCentral, puertoCentral, "LEAK_FIXED#" + idWS);
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

        int puertoMonitor;
        int puertoCentral;

        // Se debe comprobar que los puertos introducidos sean números enteros
        try {
            puertoMonitor = Integer.parseInt(args[0]);
            puertoCentral = Integer.parseInt(args[2]);
        } catch(NumberFormatException e) {
            System.out.println("Error: puerto no válido");
            return;
        }

        String ipCentral = args[1];
        String idWS = args[3];

        // Se debe registrar en la central, para ello se conecta y espera respuesta
        if(!registrar(ipCentral, puertoCentral, idWS)) {
            return;
        }

        // Se abre el servidor para el engine 
        try(ServerSocket ss = new ServerSocket(puertoMonitor)) {
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