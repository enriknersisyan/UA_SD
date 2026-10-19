package ua.es.engine;


public class Main {
    // Constantes
    private static final String PING = "PING";
    private static final String OK = "OK";
    private static final String KO = "KO";
    private static final int REINTENTO_MS = 3000;
    private static final int PERIODO_MS = 1000;

    // Enum para diferenciar los distintos estados de un engine
    public enum ESTADO { DISPONIBLE, PENDIENTE_INICIO, REGANDO, FUERA_SERVICIO, FUGA}

    public static void main(String[] args) {
        System.out.println("WM_WS_E");
    }
}