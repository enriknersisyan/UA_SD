package ua.es.engine;


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

    public static void main(String[] args) {
        System.out.println("WM_WS_E");
    }
}