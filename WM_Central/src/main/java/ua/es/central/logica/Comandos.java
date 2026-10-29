package ua.es.central.logica;

import java.util.NoSuchElementException;
import java.util.Scanner;

import ua.es.central.kafka.Productor;
import ua.es.central.modelo.Estado;

/**
 * Menu de comandos de Central (punto 11 del enunciado): iniciar un riego, bloquear y activar una estacion
 */
public class Comandos implements Runnable {
    private final RegistroEstaciones registro;
    private final Riegos riegos;
    private final Productor productor;

    /**
     * @param registro para cambiar el estado de la estacion
     * @param riegos para iniciar un riego desde Central
     * @param productor para enviar mensajes a los FO y al Engine
     */
    public Comandos(RegistroEstaciones registro, Riegos riegos, Productor productor) {
        this.registro = registro;
        this.riegos = riegos;
        this.productor = productor;
    }

    /**
     * Ejecuta el menu de comandos de Central
     */
    @Override
    public void run() {
        Scanner sc = new Scanner(System.in);
        try {
            for(;;) {
                System.out.println("\nCOMANDOS: [1] Iniciar riego  [2] Bloquear estacion  [3] Activar estacion");
                String opcion = sc.nextLine().trim();
                if(opcion.equals("1")) {
                    iniciar(sc);
                } else if(opcion.equals("2")) {
                    bloquear(pedirId(sc));
                } else if(opcion.equals("3")) {
                    activar(pedirId(sc));
                } else {
                    System.out.println("Opcion no valida");
                }
            }
        } catch(NoSuchElementException e) { // sin consola (por ejemplo en un contenedor sin terminal)
            System.out.println("Sin consola: menu de comandos desactivado");
        }
    }
    /**
     * Pide al usuario el ID de la estacion y lo devuelve. No comprueba que exista.
     */
    private String pedirId(Scanner sc) {
        System.out.println("ID de la estacion: ");
        return sc.nextLine().trim();
    }
    /**
     * Inicia un riego desde Central (sin FO al que avisar). Cambia el estado de la estacion a REGANDO y envia
     * un mensaje al Engine con la duracion del riego. Si la estacion no esta DISPONIBLE, no hace nada.
     */
    private void iniciar(Scanner sc) {
        String id = pedirId(sc);
        System.out.println("Duracion (s): ");
        try {
            int duracion = Integer.parseInt(sc.nextLine().trim());
            if(registro.estado(id) == null) {
                System.out.println("Estacion desconocida: " + id);
                return;
            }
            riegos.desdeCentral(id, duracion);
        } catch(NumberFormatException e) {
            System.out.println("Duracion no valida");
        }
    }

    /**
     * Una estacion disponible o regando pasa a FUERA_DE_SERVICIO; el Engine corta el riego si lo hay
     */
    private void bloquear(String id) {
        Estado actual = registro.estado(id);
        if(actual != Estado.DISPONIBLE && actual != Estado.REGANDO) {
            System.out.println("No se puede bloquear " + id + " (estado " + actual + ")");
            return;
        }
        /**
         * Cambia el estado de la estacion a FUERA_DE_SERVICIO y envia un mensaje al Engine para bloquearla.
         * Si el estado cambio justo ahora, no hace nada.
         */
        if(registro.cambiar(id, actual, Estado.FUERA_DE_SERVICIO)) {
            productor.enviar(Productor.TOPIC_ORDENES, id, "BLOQUEAR");
            System.out.println("Bloqueo enviado a " + id);
        } else {
            System.out.println("El estado de " + id + " cambio justo ahora, repite el comando");
        }
    }

    /**
     * Solo una estacion fuera de servicio vuelve a DISPONIBLE
     */
    private void activar(String id) {
        if(registro.cambiar(id, Estado.FUERA_DE_SERVICIO, Estado.DISPONIBLE)) {
            productor.enviar(Productor.TOPIC_ORDENES, id, "ACTIVAR");
            System.out.println("Activacion enviada a " + id);
        } else {
            System.out.println("No se puede activar " + id + " (estado " + registro.estado(id) + ")");
        }
    }
}