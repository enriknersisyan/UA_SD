package ua.es.compartido;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.CompletableFuture;

import org.junit.jupiter.api.Test;

class CanalTest {
    /**
     * Test que simula un cliente y un servidor usando la clase Canal.
     * El servidor acepta la conexion, recibe una peticion y responde con "OK".
     * El cliente envia una peticion y espera la respuesta.
     * Se verifica que el mensaje recibido por el servidor es el esperado.
     */
    @Test
    void peticionYRespuesta() throws Exception {
        try(ServerSocket ss = new ServerSocket(0)) {
            // Servidor: acepta, recibe y responde
            CompletableFuture<String> servidor = CompletableFuture.supplyAsync(() -> {
                try(Canal canal = new Canal(ss.accept())) {
                    canal.aceptar();// recibe el ENQ responde ACK

                    String peticion = canal.recibir();// recibe trama
                    canal.enviar("OK");
                    return peticion;
                } catch(Exception e) {
                    throw new RuntimeException(e);
                }
            });
            // Cliente
            try(Canal canal = new Canal(new Socket("127.0.0.1", ss.getLocalPort()))) {
                canal.abrir(); // envia ENQ y espera ACK
                assertEquals("OK", canal.peticion("REGISTER#WS-01"));
            }// sale del try y cierra el socket, el servidor recibe EOT


            assertEquals("REGISTER#WS-01", servidor.get());
        }
    }
    /**
     * Test que simula un cliente enviando una trama corrupta (con LRC incorrecto).
     * El servidor responde con NACK y el cliente reenvia la trama correctamente.
     * Se verifica que el mensaje recibido por el servidor es el esperado.
     */
    @Test
    void tramaCorruptaRecibeNackYSeAceptaElReenvio() throws Exception {
        try(ServerSocket ss = new ServerSocket(0)) {
            //servidor espera mensaje
            CompletableFuture<String> servidor = CompletableFuture.supplyAsync(() -> {
                try(Canal canal = new Canal(ss.accept())) {
                    return canal.recibir();
                } catch(Exception e) {
                    throw new RuntimeException(e);
                }
            });
            try(Socket s = new Socket("127.0.0.1", ss.getLocalPort())) {
                OutputStream out = s.getOutputStream();
                // Se crea trama con el LRC cambiado a proposito
                byte[] mala = Trama.empaquetar("LEAK#WS-01");
                mala[mala.length - 1] ^= 0x01;
                out.write(mala);
                out.flush();
                //recalcula el LRC 
                assertEquals(Trama.NACK, s.getInputStream().read());
                // Reenvio correcto
                out.write(Trama.empaquetar("LEAK#WS-01"));
                out.flush();
                assertEquals(Trama.ACK, s.getInputStream().read());
            }
            assertEquals("LEAK#WS-01", servidor.get());
        }
    }
    /**
     * Test que verifica que cuando el otro extremo cierra la conexion enviando EOT,
     * el metodo recibir() devuelve null.
     */
    @Test
    void eotTerminaLaRecepcion() throws Exception {
        try(ServerSocket ss = new ServerSocket(0)) {
            //servidor espera mensaje
            CompletableFuture<String> servidor = CompletableFuture.supplyAsync(() -> {
                try(Canal canal = new Canal(ss.accept())) {
                    return canal.recibir(); // llega EOT y devuelve null
                } catch(Exception e) {
                    throw new RuntimeException(e);
                }
            });
            //cliente se conecta y cierra el canal enviando EOT
            new Canal(new Socket("127.0.0.1", ss.getLocalPort())).close(); // envia EOT
            assertNull(servidor.get());
        }
    }
}