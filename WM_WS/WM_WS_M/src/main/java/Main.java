import java.net.Socket;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.SocketTimeoutException;

public class Main {
    // CONSTANTES
    private static String PING = "PING";
    private static String OK = "OK";
    private static String KO = "KO";
    private static int TIMEOUT_ENGINE_MS = 2000;
    private static int TIMEOUT_CENTRAL_MS = 3000;
    private static int PERIODO_MS = 1000;

    // Función para registrar al monitor en la central
    private static boolean registrar(String ip, int puerto, int id) {
        // Mensaje que espera recibir de la central
        String msg;

        // Se debe conectar a la central
        try(Socket s = new Socket(ip, puerto)) {
            BufferedReader entrada = new BufferedReader(new InputStreamReader(s.getInputStream()));
            PrintWriter salida = new PrintWriter(s.getOutputStream(), true);

            // Se añade un tiempo de timeout
            s.setSoTimeout(TIMEOUT_CENTRAL_MS);
            salida.println("REGISTER#" + id);
            msg = entrada.readLine();
        } catch(SocketTimeoutException e) {
            System.out.println("Error: no se puede conectar a la central");
            return false;
        } catch(IOException e) {
            System.out.println("Error: no se puede conectar a Central");
            return false;
        }

        if(msg != "OK") {
            return false;
        } else {
            return true;
        }
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

        // Se debe registrar en la central, para ello se conecta y espera respuesta
        try(Socket s = new Socket(args[1], puertoCentral)) {

        } catch(IOException e) {
            System.out.println("Error: no se puede conectar a la central.");
            return;
        }
    }
}