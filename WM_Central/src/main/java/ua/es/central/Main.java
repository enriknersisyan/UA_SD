package ua.es.central;
import java.sql.SQLException;

import ua.es.central.basedatos.Database;
import ua.es.central.basedatos.EstacionBD;
import ua.es.central.basedatos.OperarioBD;
import ua.es.central.modelo.Estacion;
import ua.es.central.modelo.Estado;

import ua.es.central.red.ServidorSockets;
import ua.es.central.logica.RegistroEstaciones;

import java.io.IOException;
import ua.es.central.basedatos.DatosIniciales;

public class Main {
    
    public static void main(String[] args) {
        // solo puede haber un argumento que es el puerto de sockets
        if(args.length != 1) {
            System.out.println("Uso: java -jar WM_Central.jar <puertoSockets>");
            System.exit(1);
            return;
        }
    
    
        int socketPort;//hay que cambiar el tipo de argumentos
        try{
            socketPort = Integer.parseInt(args[0]);

        } catch(NumberFormatException er){
            System.out.println("Error: el puerto no es un numero: " + args[0]);
            System.exit(1);
            return;
        }
        // Puerto Tcp valido
        if(socketPort < 1 || socketPort > 65535){
            System.out.println("Error: el puerto no es entre 1 y 65535 ");
            System.exit(1);
            return;
        }
        RegistroEstaciones registro;
        // Se abre la base de datos; si falla, Central no puede continuar.
        try {
            Database baseDatos = new Database();
            EstacionBD estaciones = new EstacionBD(baseDatos);
            OperarioBD operarios = new OperarioBD(baseDatos);
            DatosIniciales.cargar(estaciones, operarios);
            estaciones.desconectarTodas();

            registro = new RegistroEstaciones(estaciones);
            registro.cargar(estaciones.listar());
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
    
        System.out.println("WM_Central arrancado");
        System.out.println("Puerto de sockets :" + socketPort);
        new ServidorSockets(socketPort, registro).arrancar();
     
    }
}