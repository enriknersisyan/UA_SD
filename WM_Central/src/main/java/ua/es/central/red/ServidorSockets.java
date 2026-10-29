package ua.es.central.red;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.net.SocketTimeoutException;


import ua.es.central.logica.RegistroEstaciones;
import ua.es.compartido.Canal;
/**
 * Servidor de sockets que atiende a estaciones de trabajo
 */
public class ServidorSockets{
	private static final int TIMEOUT_LECTURA_MS = 3000;
    private static final int TIMEOUT_LATIDO_MS = 6000;

    private final int puerto;
    private final RegistroEstaciones registro;

    /**
     * Crea un servidor de sockets en el puerto indicado y con el registro de estaciones dado
     * @param puerto puerto en el que escuchar
     * @param registro registro de estaciones
     */
    public ServidorSockets(int puerto, RegistroEstaciones registro) {
        this.puerto = puerto;
        this.registro = registro;
    }

    /**
     * Arranca el servidor de sockets y atiende a los clientes en hilos virtuales
     */
    public void arrancar(){
        try(ServerSocket ss = new ServerSocket(puerto);
        ExecutorService hilos = Executors.newVirtualThreadPerTaskExecutor()){
            for(;;) {
                Socket s = ss.accept();
                hilos.execute(() -> atender(s));
            }
        }catch(IOException e){
            System.out.println("Error: no se puede abrir el puerto " + puerto);
            System.exit(1);
        }
    }
    /**
    * Atiende a un cliente en un hilo virtual
     * @param s socket del cliente
     */
    private void atender(Socket s) {
        String id = null;
        try(s; Canal canal = new Canal(s)){
            
            s.setSoTimeout(TIMEOUT_LECTURA_MS);
            canal.aceptar();
            String linea = canal.recibir();
            if(linea == null) { return;}

            String respuesta = registro.registrar(linea);
            System.out.println("Recibido " + linea + " ->" + respuesta);
            if("OK".equals(respuesta)){ id = linea.split("#")[1]; } // antes de enviar, para que el finally la libere si el envio falla
            canal.enviar(respuesta);
            if(id == null){ return; }

            s.setSoTimeout(TIMEOUT_LATIDO_MS);
            while((linea = canal.recibir()) != null){
                respuesta = registro.procesar(id, linea);
                if(!linea.startsWith("STATUS#")){
                    System.out.println("Recibido "+ linea + " -> " + respuesta);
                }
                canal.enviar(respuesta);
            }
        }catch(SocketTimeoutException e){
                System.out.println("Estacion " + id + ": sin latidos, se da por caida");
        }catch(IOException e){
            System.out.println("Error atendiendo conexion: "+ e.getMessage());
        }finally{
            if(id != null){
                registro.desconectar(id);
                System.out.println("Estacion " + id + " desconectada");                
            }
        }            
    }
}