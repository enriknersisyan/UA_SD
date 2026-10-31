package ua.es.central.logica;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BiConsumer;

import ua.es.central.modelo.Estacion;
import ua.es.central.modelo.Estado;
import ua.es.central.basedatos.EstacionBD;

public class RegistroEstaciones {
	 private final Map<String, Estado> estados = new ConcurrentHashMap<>();
    
     private final EstacionBD bd;
     private final List<BiConsumer<String, Estado>> oyentes = new CopyOnWriteArrayList<>();

     /**
      * Registra a alguien que quiere enterarse de cada cambio de estado de una estacion
      * (el panel, la logica de riegos...). Puede haber varios.
      */
     public void alCambiar(BiConsumer<String, Estado> oyente) {
        oyentes.add(oyente);
     }

     /** 
      * Crea un registro de estaciones con la base de datos dada
      * @param bd base de datos de estaciones
     */    
     public RegistroEstaciones(EstacionBD bd) {
        this.bd = bd;
     }
	 /**
     * Carga el registro de estaciones con las estaciones dadas, inicializando su estado a DESCONECTADA
     **/
     public void cargar(List<Estacion> estaciones) {
        for(Estacion e : estaciones) {
            estados.put(e.id(), Estado.DESCONECTADA);
        }
    }
    /**
     * Registra una estacion en el registro de estaciones
     * @param linea linea recibida del monitor
     * @return "OK" si se ha registrado correctamente, "KO" en caso contrario
     */
    public String registrar(String linea){
        String[] partes = linea.split("#");
        if(partes.length !=2 || !partes[0].equals("REGISTER")) {
            return "KO";
        }
        String id = partes[1];

        //No esta la estacion en al bd
        if(!estados.containsKey(id)) {
            return "KO";
        }
        // si dos monitores llegan gana uno, KO si ya hay uno conectado
        if(!estados.replace(id, Estado.DESCONECTADA, Estado.DISPONIBLE)){
            return "KO";
        }
        
        guardar(id, Estado.DISPONIBLE);
        return "OK";
    }


    /**
     * Procesa un mensaje de una estacion
     * @param id id de la estacion
     * @param linea linea recibida del monitor
     * @return "OK" si se ha procesado correctamente, "KO" en caso contrario
     */
    public String procesar(String id, String linea) {
        String[] partes = linea.split("#");
        if(partes.length != 2 || !partes[1].equals(id)) {
            return "KO";
        }

        String comando = partes[0];
        switch(comando) {
            case "STATUS":  break;
            case "LEAK":       estados.put(id, Estado.FUGA); guardar(id, Estado.FUGA);      break;
            case "LEAK_FIXED": if(estados.replace(id, Estado.FUGA, Estado.DISPONIBLE)) {
                                    guardar(id, Estado.DISPONIBLE);}      break;

            default:           return "KO";
        }
        return "OK";
    }
    /**
     * Devuelve el estado de la estacion con el id dado
     * @param id id de la estacion
     * @return estado de la estacion, o null si no existe
     */
    public Estado estado(String id) {
        return estados.get(id);
    }
    /** Cambia el estado solo si ahora es "desde" (atomico), y lo guarda en la BD */
    public boolean cambiar(String id, Estado desde, Estado hacia) {
        if(!estados.replace(id, desde, hacia)) {
            return false;
        }
        guardar(id, hacia);
        return true;
    }
    /**
     * Da de alta una estacion nueva. Queda DESCONECTADA hasta que se registre su Monitor.
     * @return false si ya existe una estacion con ese id
     */
    public boolean anadir(String id, String ubicacion) throws SQLException {
        if(estados.putIfAbsent(id, Estado.DESCONECTADA) != null) {
            return false;
        }
        try {
            bd.insertar(new Estacion(id, ubicacion, Estado.DESCONECTADA));
        } catch(SQLException e) {
            estados.remove(id); // no se pudo guardar: se deshace
            throw e;
        }
        return true;
    }

    /**
     * Borra una estacion, solo si esta DESCONECTADA (sin Monitor ni riego). El borrado es atomico:
     * si a la vez se registra su Monitor, solo uno de los dos lo consigue.
     * @return false si no existe o no esta desconectada
     */
    public boolean borrar(String id) throws SQLException {
        if(!estados.remove(id, Estado.DESCONECTADA)) {
            return false;
        }
        bd.borrar(id);
        return true;
    }

    /**
     * Cambia la ubicacion de una estacion (se puede en cualquier estado)
     * @return false si no existe
     */
    public boolean cambiarUbicacion(String id, String ubicacion) throws SQLException {
        if(!estados.containsKey(id)) {
            return false;
        }
        return bd.cambiarUbicacion(id, ubicacion);
    }

    /**
     * Cambia el estado de la estacion a DESCONECTADA y lo guarda en la BD
     * @param id id de la estacion
     */
    public void desconectar(String id){
        guardar(id, Estado.DESCONECTADA);
        estados.put(id, Estado.DESCONECTADA);
    }

    /**
     * Guarda el estado de la estacion en la base de datos
     * @param id id de la estacion
     * @param estado estado de la estacion
     */
    private void guardar(String id, Estado estado) {
        try {
            bd.actualizarEstado(id, estado);
        } catch (SQLException e) {
            System.out.println("Error al actualizar el estado de la estacion " + id + ": " + e.getMessage());
        }
        for(BiConsumer<String, Estado> oyente : oyentes) {
            oyente.accept(id, estado);
        }
    }   
}