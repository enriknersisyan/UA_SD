package ua.es.engine;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
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
        System.out.println("WM_WS_E");
    }
}