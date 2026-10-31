# WaterManagement - Sistemas Distribidos 26/27

Práctica de **Sistemas Distribuidos (curso 26/27)**: simulación de un sistema de gestión
centralizada del riego de los parques y jardines de una ciudad.

Una **CENTRAL** da de alta, controla y monitoriza una red de **estaciones de riego (WS)**,
mientras los **operarios de campo** solicitan riegos desde su aplicación. Todo el sistema
funciona de forma asíncrona y en tiempo real, combinando **sockets** (registro y salud de las
estaciones) y **Kafka** (peticiones, órdenes, telemetría y resultados de los riegos).

## Módulos

| Módulo | Descripción |
|---|---|
| `WM_Central` | Lógica y gobierno del sistema. Base de datos SQLite, servidor de sockets para los monitores, productor y consumidor de Kafka y panel web de monitorización |
| `WM_WS_M` | Monitor de cada estación. Se registra en CENTRAL, comprueba cada segundo la salud de su Engine y avisa de las averías (fugas) |
| `WM_WS_E` | Engine de cada estación. Abre y cierra la electroválvula, simula el caudalímetro y envía la telemetría de cada riego |
| `WM_FO` | Aplicación de los operarios. Solicita riegos, de forma manual o desde un archivo, y muestra su evolución |
| `WM_Compartido` | Protocolo de sockets común `<STX><DATA><ETX><LRC>` con `ENQ`/`ACK`/`NACK`/`EOT` |

## Tecnologías

Java 21 · Maven · Apache Kafka · SQLite · Docker · Railway

## Despliegue

- **Railway:** `WM_Central`, base de datos y Kafka.
- **PC LAB 1:** varios `WM_FO`.
- **PC LAB 2:** varias estaciones (`WM_WS_M` + `WM_WS_E`).

## Reparto del trabajo

| Integrante | Responsabilidades |
|---|---|
| **Juan** | `WM_WS_E` (Engine de las estaciones de riego) |
| | `WM_WS_M` (Monitor de las estaciones de riego) |
| | `WM_FO` (aplicación de los operarios de campo) |
| | Panel web de monitorización de CENTRAL (HTML, CSS, JavaScript y su integración con CENTRAL) |
| **Enrik** | `WM_Central` (lógica central, base de datos y servidor de sockets) |
| | Integración con Kafka |
| | Despliegue con Docker y Railway |
| | Corrección de errores críticos |
