package ua.es.central.logica;

import java.sql.SQLException;
import java.util.NoSuchElementException;
import java.util.Scanner;

import ua.es.central.kafka.Productor;
import ua.es.central.modelo.Estado;
import ua.es.central.web.EstadoPanel;

/**
 * Menu de comandos de Central (punto 11 del enunciado): iniciar un riego, bloquear y activar una estacion
 */
public class Comandos implements Runnable {
    // Resultado de una orden de gestion: si salio bien y el mensaje para mostrar
    public record Resultado(boolean ok, String mensaje) {}

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
                System.out.println("          [4] Anadir estacion  [5] Cambiar ubicacion  [6] Borrar estacion");
                String opcion = sc.nextLine().trim();
                if(opcion.equals("1")) {
                    iniciar(sc);
                } else if(opcion.equals("2")) {
                    bloquear(pedirId(sc));
                } else if(opcion.equals("3")) {
                    activar(pedirId(sc));
                } else if(opcion.equals("4")) {
                    String id = pedirId(sc);
                    System.out.println("Ubicacion: ");
                    System.out.println(anadirEstacion(id, sc.nextLine().trim()).mensaje());
                } else if(opcion.equals("5")) {
                    String id = pedirId(sc);
                    System.out.println("Nueva ubicacion: ");
                    System.out.println(cambiarUbicacion(id, sc.nextLine().trim()).mensaje());
                } else if(opcion.equals("6")) {
                    System.out.println(borrarEstacion(pedirId(sc)).mensaje());
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

    // ---------- Gestion de estaciones (alta, cambio de ubicacion y baja) ----------

    // IDs con letras, numeros, guion o guion bajo: el # romperia los mensajes de los sockets y de Kafka
    private static boolean idValido(String id) {
        return id != null && id.matches("[A-Za-z0-9_-]{1,20}");
    }

    // Ubicacion no vacia y sin # ni ; (separadores de los mensajes y del CSV)
    private static boolean ubicacionValida(String ubicacion) {
        return ubicacion != null && !ubicacion.isBlank() && ubicacion.length() <= 60
            && !ubicacion.contains("#") && !ubicacion.contains(";");
    }

    /**
     * Da de alta una estacion nueva. Aparece DESCONECTADA hasta que se registre su Monitor.
     */
    public Resultado anadirEstacion(String id, String ubicacion) {
        if(!idValido(id)) {
            return new Resultado(false, "ID no valido: solo letras, numeros, - y _ (maximo 20)");
        }
        if(!ubicacionValida(ubicacion)) {
            return new Resultado(false, "Ubicacion no valida: no puede estar vacia ni llevar # o ;");
        }
        try {
            if(!registro.anadir(id, ubicacion.trim())) {
                return new Resultado(false, "Ya existe una estacion " + id);
            }
        } catch(SQLException e) {
            return new Resultado(false, "Error de la base de datos: " + e.getMessage());
        }
        panel.cargar(id, ubicacion.trim());
        panel.anotar("Estacion " + id + " dada de alta en " + ubicacion.trim());
        return new Resultado(true, "Estacion " + id + " dada de alta");
    }

    /**
     * Cambia la ubicacion de una estacion
     */
    public Resultado cambiarUbicacion(String id, String ubicacion) {
        if(!ubicacionValida(ubicacion)) {
            return new Resultado(false, "Ubicacion no valida: no puede estar vacia ni llevar # o ;");
        }
        try {
            if(!registro.cambiarUbicacion(id, ubicacion.trim())) {
                return new Resultado(false, "No existe la estacion " + id);
            }
        } catch(SQLException e) {
            return new Resultado(false, "Error de la base de datos: " + e.getMessage());
        }
        panel.cambiarUbicacion(id, ubicacion.trim());
        panel.anotar("Estacion " + id + " ahora esta en " + ubicacion.trim());
        return new Resultado(true, "Ubicacion de " + id + " cambiada");
    }

    /**
     * Borra una estacion. Solo si esta DESCONECTADA: con su Monitor conectado o regando no se puede.
     */
    public Resultado borrarEstacion(String id) {
        Estado actual = registro.estado(id);
        if(actual == null) {
            return new Resultado(false, "No existe la estacion " + id);
        }
        try {
            if(!registro.borrar(id)) {
                return new Resultado(false, "Solo se puede borrar una estacion desconectada (" + id + " esta " + registro.estado(id) + ")");
            }
        } catch(SQLException e) {
            return new Resultado(false, "Error de la base de datos: " + e.getMessage());
        }
        panel.quitar(id);
        panel.anotar("Estacion " + id + " borrada");
        return new Resultado(true, "Estacion " + id + " borrada");
    }
}