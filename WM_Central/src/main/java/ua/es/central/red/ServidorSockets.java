package ua.es.central.red;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.net.SocketTimeoutException;


import ua.es.central.logica.RegistroEstaciones;


public class ServidorSockets{
	private static final int TIMEOUT_LECTURA_MS = 3000;
    private static final int TIMEOUT_LATIDO_MS = 6000;

    private final int puerto;
    private final RegistroEstaciones registro;

    public ServidorSockets(int puerto, RegistroEstaciones registro) {
        this.puerto = puerto;
        this.registro = registro;
    }
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
    private void atender(Socket s) {
        String id = null;
        try(s; BufferedReader entrada = new BufferedReader(new InputStreamReader(s.getInputStream()));
            PrintWriter salida = new PrintWriter(s.getOutputStream(), true)){
            
            s.setSoTimeout(TIMEOUT_LECTURA_MS);
            String linea = entrada.readLine();
            if(linea == null) { return;}

            String respuesta = registro.registrar(linea);
            System.out.println("Recibido " + linea + " ->" + respuesta);
            salida.println(respuesta);
            if(!"OK".equals(respuesta)){ return; }
                
            id = linea.split("#")[1];

            s.setSoTimeout(TIMEOUT_LATIDO_MS);
            while((linea = entrada.readLine()) != null){
                respuesta = registro.procesar(id, linea);
                if(!linea.startsWith("STATUS#")){
                    System.out.println("Recibido "+ linea + " -> " + respuesta);
                }
                salida.println(respuesta);
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