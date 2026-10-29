package ua.es.compartido;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;

public final class Trama {
	public static final byte STX  = 0x02; // inicio de texto
    public static final byte ETX  = 0x03; // fin de texto
    public static final byte EOT  = 0x04; // fin de la comunicacion
    public static final byte ENQ  = 0x05; // peticion para empezar a hablar
    public static final byte ACK  = 0x06; // recibido bien
    public static final byte NACK = 0x15; // recibido mal, repitelo

    private Trama() {
        // Constructor privado para evitar instanciacion 
    }
    public static byte lrc(byte[] datos){
        byte resultado = 0;
        for(byte b : datos) {
            resultado ^= b;
        }
        return resultado;
    }


    // Convierte un mensaje en la trama completa lista para enviar
    public static byte[] empaquetar(String mensaje){
        byte[] datos = mensaje.getBytes(StandardCharsets.UTF_8);
        ByteArrayOutputStream trama = new ByteArrayOutputStream(datos.length + 3);
        trama.write(STX);
        trama.writeBytes(datos);
        trama.write(ETX);
        trama.write(lrc(datos));
        return trama.toByteArray();

    }   
}