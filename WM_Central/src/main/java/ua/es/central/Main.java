package ua.es.central;

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
    
        System.out.println("WM_Central arrancado");
        System.out.println("Puerto de sockets :" + socketPort);

       
    }
}