package ua.es.compartido;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;

public class Canal implements Closeable {
	private static final int TIMEOUT_ACK_MS = 1000;
	private static final int MAX_REENVIOS = 2;
	private static final int MAX_DATOS = 4096;

	private final Socket socket;
	private final InputStream entrada;
	private final OutputStream salida;
	

	public Canal(Socket socket) throws IOException {
		this.socket = socket;
		this.entrada = socket.getInputStream();
		this.salida = socket.getOutputStream();
	}
	public void abrir() throws IOException{
		salida.write(Trama.ENQ);
		salida.flush();
		int respuesta = leerConTimeout(TIMEOUT_ACK_MS);
		if(respuesta != Trama.ACK) {
			throw new IOException("No se recibio ACK al abrir el canal");
		}
	}


	public void aceptar() throws IOException{
		int b = entrada.read();
		if(b == -1){
			throw new EOFException("Fin de flujo inesperado al aceptar canal");
		}
		if(b != Trama.ENQ){
			salida.write(Trama.NACK);
			salida.flush();
			throw new IOException("Se esperaba ENQ para aceptar canal, pero se recibio: " + b);
		}
		salida.write(Trama.ACK);
		salida.flush();
	}

	// Envia un mensaje y espera su ACK. Reenvia tras NACK o sin respuesta.
    public synchronized void enviar(String mensaje) throws IOException {
        byte[] trama = Trama.empaquetar(mensaje);
        for(int intento = 0; intento <= MAX_REENVIOS; intento++) {
            salida.write(trama);
            salida.flush();
            int respuesta;
            try {
                respuesta = leerConTimeout(TIMEOUT_ACK_MS);
            } catch(SocketTimeoutException e) {
                continue; // no ha llegado el ACK a tiempo: se reenvia
            }
            if(respuesta == Trama.ACK) {
                return;
            }
            if(respuesta == -1) {
                throw new EOFException("conexion cerrada esperando ACK");
            }
            // NACK (o cualquier otra cosa): se reenvia
        }
        throw new IOException("sin ACK tras " + (MAX_REENVIOS + 1) + " intentos");
    }

	// Espera un mensaje. Responde ACK si el LRC cuadra y NACK si no (y espera el reenvio).
    // Devuelve null si el otro extremo termina (EOT o cierre limpio).
    public String recibir() throws IOException {
        for(;;) {
            // Buscar el inicio de la trama
            int b = entrada.read();
            if(b == -1 || b == Trama.EOT) {
                return null;
            }
            if(b != Trama.STX) {
                continue; // byte suelto que no es una trama: se ignora
            }

            // Leer el contenido hasta ETX
            ByteArrayOutputStream datos = new ByteArrayOutputStream();
            while((b = entrada.read()) != Trama.ETX) {
                if(b == -1) {
                    throw new EOFException("conexion cerrada a mitad de trama");
                }
                if(datos.size() >= MAX_DATOS) {
                    throw new IOException("trama demasiado larga");
                }
                datos.write(b);
            }

            // El byte que sigue a ETX es siempre el LRC (aunque valga lo mismo que un caracter de control)
            int lrcRecibido = entrada.read();
            if(lrcRecibido == -1) {
                throw new EOFException("conexion cerrada antes del LRC");
            }

            byte[] contenido = datos.toByteArray();
            if((byte) lrcRecibido == Trama.lrc(contenido)) {
                salida.write(Trama.ACK);
                salida.flush();
                return new String(contenido, StandardCharsets.UTF_8);
            }
            salida.write(Trama.NACK); // corrupta: se pide el reenvio y se vuelve a esperar
            salida.flush();
        }
    }

    // Peticion completa del lado cliente: envia y espera la respuesta.
    // synchronized para que dos hilos que comparten el canal no mezclen sus mensajes.
    public synchronized String peticion(String mensaje) throws IOException {
        enviar(mensaje);
        return recibir();
    }

    // Cierre ordenado: EOT y se cierra el socket
    @Override
    public void close() throws IOException {
        try {
            if(!socket.isClosed()) {
                salida.write(Trama.EOT);
                salida.flush();
            }
        } catch(IOException e) {
            // el otro extremo ya no esta: no importa
        } finally {
            socket.close();
        }
    }

    // Lee un byte con un plazo concreto y restaura despues el plazo que tuviera el socket
    private int leerConTimeout(int ms) throws IOException {
        int anterior = socket.getSoTimeout();
        socket.setSoTimeout(ms);
        try {
            return entrada.read();
        } finally {
            socket.setSoTimeout(anterior);
        }
    }
}