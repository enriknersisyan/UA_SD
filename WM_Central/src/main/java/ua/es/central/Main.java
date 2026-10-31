package ua.es.central;
import java.sql.SQLException;

import ua.es.central.basedatos.Database;
import ua.es.central.basedatos.EstacionBD;
import ua.es.central.basedatos.OperarioBD;
import ua.es.central.modelo.Estacion;
import ua.es.central.logica.Comandos;
import ua.es.central.logica.Riegos;
import ua.es.central.web.EstadoPanel;
import ua.es.central.web.PanelWeb;


import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import ua.es.central.kafka.Consumidor;
import ua.es.central.kafka.Productor;

import ua.es.central.red.ServidorSockets;
import ua.es.central.logica.RegistroEstaciones;

import java.io.IOException;
import ua.es.central.basedatos.DatosIniciales;

public class Main {
    /**
     * Periodo de publicacion de estados en segundos
     */
    private static final int PERIODO_ESTADOS_S = 2;
    
    /**
     * Lee un puerto de la linea de comandos y lo valida
     * @param texto puerto en formato String
     * @return puerto en formato int
     */
    private static int leerPuerto(String texto) {
        int puerto;
        try {
            puerto = Integer.parseInt(texto);
        } catch(NumberFormatException e) {
            puerto = 0;
        }
        if(puerto < 1 || puerto > 65535) {
            System.out.println("Error: puerto no valido (1-65535): " + texto);
            System.exit(1);
        }
        return puerto;
    }

    /**
     * Publica el estado de todas las estaciones en Kafka
     * @param productor productor de Kafka
     * @param estaciones base de datos de estaciones
     * @param registro registro de estados
     */
    private static void publicarEstados(Productor productor, EstacionBD estaciones, RegistroEstaciones registro) {
        try {
            for(Estacion e : estaciones.listar()) {
                productor.enviar(Productor.TOPIC_ESTADOS, e.id(),
                    "ESTADO#" + e.id() + "#" + e.ubicacion() + "#" + registro.estado(e.id()));
            }
        } catch(Exception e) { // si esta tarea periodica lanza una excepcion, deja de repetirse sin avisar
            System.out.println("Error publicando estados: " + e.getMessage());
        }
    }
    /**
     * Arranca Central: base de datos, registro de estaciones, productor y consumidor de Kafka, servidor de sockets
     * @param args puertoSockets ipBroker puertoBroker
     */
    public static void main(String[] args) {
        if(args.length != 3) {
            System.out.println("Uso: java -jar WM_Central.jar <puertoSockets> <ipBroker> <puertoBroker>");
            System.exit(1);
            return;
        }
        int socketPort = leerPuerto(args[0]);
        String broker = args[1] + ":" + leerPuerto(args[2]);

        RegistroEstaciones registro;
        EstacionBD estaciones;
        OperarioBD operarios;
        EstadoPanel panel = new EstadoPanel();
        
        // Se abre la base de datos; si falla, Central no puede continuar.
        try {
            Database baseDatos = new Database();
            estaciones = new EstacionBD(baseDatos);
            operarios = new OperarioBD(baseDatos);
            DatosIniciales.cargar(estaciones, operarios);
            estaciones.desconectarTodas();

            registro = new RegistroEstaciones(estaciones);
            registro.cargar(estaciones.listar());
            for(Estacion e : estaciones.listar()) {
                panel.cargar(e.id(), e.ubicacion());
            }
            // El panel se entera de cada cambio de estado de una estacion
            registro.alCambiar((id, estado) -> {
                panel.cambiarEstado(id, estado.name());
                panel.anotar(id + " -> " + estado);
            });



            System.out.println("Estaciones registradas: " + estaciones.listar().size());
        } catch (SQLException e) {
            System.out.println("Error con la base de datos: " + e.getMessage());
            System.exit(1);
            return;
        } catch (IOException e) {
            System.out.println("Error al cargar datos iniciales: " + e.getMessage());
            System.exit(1);
            return;
        }

        /** 
         * Kafka: un productor compartido, un hilo consumidor y la foto periodica de estados 
         */
        Productor productor = new Productor(broker);
        productor.alCambiarConexion(panel::kafka);
        Runtime.getRuntime().addShutdownHook(new Thread(productor::close));
        
        Riegos riegos = new Riegos(registro, operarios, productor, panel);
        registro.alCambiar(riegos::cambioEstado); // avisar al FO si su estacion se averia durante el riego
        new Thread(new Consumidor(broker, riegos)).start();
        Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(
            () -> publicarEstados(productor, estaciones, registro), 0, PERIODO_ESTADOS_S, TimeUnit.SECONDS);
        Comandos comandos = new Comandos(registro, riegos, productor, panel);
        new Thread(comandos).start();

        // Panel web: Railway indica el puerto en la variable PORT; en local se usa el 8080
        String puertoTexto = System.getenv("PORT");
        int webPort = (puertoTexto == null || puertoTexto.isBlank()) ? 8080 : leerPuerto(puertoTexto);
        try {
            new PanelWeb(webPort, panel, comandos).arrancar();
        } catch(IOException e) {
            System.out.println("Error: no se puede abrir el panel web en el puerto " + webPort + ": " + e.getMessage());
        }


        System.out.println("WM_Central arrancado");
        System.out.println("Puerto de sockets :" + socketPort);
        System.out.println("Broker Kafka :" + broker);
        new ServidorSockets(socketPort, registro).arrancar();
    }
}