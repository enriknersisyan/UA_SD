package ua.es.central.logica;

import java.util.NoSuchElementException;
import java.util.Scanner;

import ua.es.central.kafka.Productor;
import ua.es.central.modelo.Estado;
import ua.es.central.web.EstadoPanel;

/**
 * Menu de comandos de Central (punto 11 del enunciado): iniciar un riego, bloquear y activar una estacion
 */
public class Comandos implements Runnable {
    private final RegistroEstaciones registro;
    private final Riegos riegos;
    private final Productor productor;
    private final EstadoPanel panel;

    /**
     * @param registro para cambiar el estado de la estacion
     * @param riegos para iniciar un riego desde Central
     * @param productor para enviar mensajes a los FO y al Engine
     * @param panel para mostrar el estado de las estaciones
     */
    public Comandos(RegistroEstaciones registro, Riegos riegos, Productor productor, EstadoPanel panel) {
        this.registro = registro;
        this.riegos = riegos;
        this.productor = productor;
        this.panel = panel;
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
     * Pide al usuario el ID de la estacion y la duracion del riego, y llama a iniciar(id, duracion)
     */
    private void iniciar(Scanner sc) {
        String id = pedirId(sc);
        System.out.println("Duracion (s): ");
        try {
            iniciar(id, Integer.parseInt(sc.nextLine().trim()));
        } catch(NumberFormatException e) {
            System.out.println("Duracion no valida");
        }
    }

    /**
     * Inicia un riego desde Central. Lo usan el menu y el panel web.
     */
    public void iniciar(String id, int duracion) {
        if(registro.estado(id) == null) {
            panel.anotar("Estacion desconocida: " + id);
            return;
        }
        riegos.desdeCentral(id, duracion);
    }

    /**
     * Una estacion disponible o regando pasa a FUERA_DE_SERVICIO; el Engine corta el riego si lo hay
     */
    public void bloquear(String id) {
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
            panel.anotar("Bloqueo enviado a " + id);
        } else {
            panel.anotar("El estado de " + id + " cambio justo ahora, repite el comando");
        }
    }

    /**
     * Solo una estacion fuera de servicio vuelve a DISPONIBLE
     */
    public void activar(String id) {
        if(registro.cambiar(id, Estado.FUERA_DE_SERVICIO, Estado.DISPONIBLE)) {
            productor.enviar(Productor.TOPIC_ORDENES, id, "ACTIVAR");
            panel.anotar("Activacion enviada a " + id);
        } else {
            panel.anotar("No se puede activar " + id + " (estado " + registro.estado(id) + ")");
        }
    }
}