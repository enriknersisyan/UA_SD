// ==========================================================
//  PANEL DE CENTRAL – LÓGICA
//
//  Habla con CENTRAL (PanelWeb.java) por HTTP:
//
//  GET /api/estado  ->
//  {
//    "central": {
//      "kafka": true,                    // ¿CENTRAL está conectada a Kafka?
//      "arranque": "27/10/2026 10:00:12" // cuándo arrancó CENTRAL
//    },
//    "estaciones": [
//      { "id": "WS-01", "ubicacion": "Parque del Río",
//        "estado": "DISPONIBLE" | "REGANDO" | "FUGA" | "FUERA_DE_SERVICIO" | "DESCONECTADA",
//        "caudal": 11.4, "volumen": 186.0, "operario": "FO-12" | null,
//        "ultimoRiego": "12:04:11" | null }
//    ],
//    "peticiones": [                     // activas y últimas terminadas, la más antigua primero
//      { "id": 7, "hora": "12:03:58", "operario": "FO-12", "ws": "WS-01", "duracion": 30,
//        "fase": "RECIBIDA" | "AUTORIZADA" | "REGANDO" | "TERMINADA" | "DENEGADA",
//        "resultado": "12,4 L en 30 s (LIMITE_TIEMPO)" | "NO_DISPONIBLE" | "" }
//    ],
//    "eventos": [                        // la más antigua primero
//      { "id": 57, "hora": "12:04:11", "texto": "WS-01 → fuga detectada" }
//    ]
//  }
//
//  POST /api/orden <- accion=INICIAR|BLOQUEAR|ACTIVAR & idWS=WS-01|TODAS & duracion=30
// ==========================================================

// ---------------- Configuración ----------------
const API_ESTADO = "/api/estado";
const API_ORDEN  = "/api/orden";
const PERIODO_MS = 1000;            // cada segundo (punto 7, pág. 7)
const MAX_EVENTOS = 100;

// Los 5 estados del enunciado (pág. 4), con los mismos nombres que usa el Engine
const ETIQUETAS = {
  DISPONIBLE:     "Disponible",
  REGANDO:        "Regando",
  FUGA:           "Fuga",
  FUERA_DE_SERVICIO: "Fuera de Servicio",
  DESCONECTADA:   "Desconectada"
};

// Fases de una petición de riego (puntos 4 a 8, pág. 7)
const FASES = {
  RECIBIDA:   "Recibida",     // CENTRAL la está comprobando
  AUTORIZADA: "Autorizada",   // la WS ha aceptado, falta abrir la válvula
  REGANDO:    "Regando",
  TERMINADA:  "Terminada",
  DENEGADA:   "Denegada"
};
const FASES_FINALES = ["TERMINADA", "DENEGADA"];

// ---------------- Estado de la página ----------------
let estaciones = [];                // lista de estaciones
let peticiones = [];                // lista de peticiones
let central = {};                   // estado del sistema central
let eventos = [];                   // [{hora, texto}], el más reciente primero
const eventosVistos = new Set();    // ids de eventos ya recibidos
let seleccion = null;               // id de la WS seleccionada
let conectado = false;              // ¿el navegador llega a CENTRAL?
let ultimaActualizacion = null;     // hora de la última respuesta correcta

// ---------------- Utilidades ----------------
const $ = (id) => document.getElementById(id);

function formatear(n, dec = 1) {
  if (n === null || n === undefined || isNaN(n)) return "—";
  return Number(n).toLocaleString("es-ES", { minimumFractionDigits: dec, maximumFractionDigits: dec });
}

// textContent: los datos recibidos nunca se interpretan como HTML
function crear(etiqueta, texto, clase) {
  const el = document.createElement(etiqueta);
  if (texto !== undefined) el.textContent = texto;
  if (clase) el.className = clase;
  return el;
}

function etiquetaEstado(estado) {
  return crear("span", ETIQUETAS[estado] || estado, "estado estado-" + String(estado).toLowerCase());
}

function etiquetaFase(fase) {
  return crear("span", FASES[fase] || fase, "fase fase-" + String(fase).toLowerCase());
}

// Añade una pareja "nombre: valor" a una lista <dl>
function filaDatos(dl, nombre, valor, claseValor) {
  dl.append(crear("dt", nombre), crear("dd", valor, claseValor));
}

// ---------------- Leer el estado de CENTRAL ----------------
async function actualizar() {
  try {
    const r = await fetch(API_ESTADO, { cache: "no-store" });
    if (!r.ok) throw new Error("HTTP " + r.status);
    const datos = await r.json();

    central    = datos.central || {};
    estaciones = Array.isArray(datos.estaciones) ? datos.estaciones : [];
    peticiones = Array.isArray(datos.peticiones) ? datos.peticiones : [];

    // CENTRAL envía los eventos del más antiguo al más reciente:
    // se añaden arriba solo los que no se habían visto
    for (const e of (datos.eventos || [])) {
      if (eventosVistos.has(e.id)) continue;
      eventosVistos.add(e.id);
      eventos.unshift({ hora: e.hora, texto: e.texto });
    }
    eventos = eventos.slice(0, MAX_EVENTOS);

    ultimaActualizacion = new Date().toLocaleTimeString("es-ES");
    marcarConexion(true);
  } catch (err) {
    marcarConexion(false);   // se mantienen los últimos datos recibidos
  }
  pintar();
}

function marcarConexion(ok) {
  conectado = ok;
  const el = $("conexion");
  el.className = "conexion " + (ok ? "ok" : "error");
  el.textContent = ok ? "Conectado a CENTRAL" : "Sin conexión con CENTRAL";
  $("aviso").hidden = ok;
}

// ---------------- Enviar órdenes (punto 11, pág. 8) ----------------
async function enviarOrden(accion) {
  const destino = document.querySelector('input[name="destino"]:checked').value;
  const idWS = destino === "todas" ? "TODAS" : seleccion;

  if (!idWS) {
    mostrarRespuesta("Selecciona una estación en la tabla o elige «Todas».");
    return;
  }

  const cuerpo = new URLSearchParams({ accion, idWS });

  if (accion === "INICIAR") {
    const duracion = parseInt($("duracion").value, 10);
    if (!Number.isInteger(duracion) || duracion <= 0) {
      mostrarRespuesta("Escribe una duración en segundos mayor que 0.");
      $("duracion").focus();
      return;
    }
    cuerpo.append("duracion", String(duracion));
  }

  try {
    const r = await fetch(API_ORDEN, { method: "POST", body: cuerpo });
    const datos = await r.json().catch(() => ({}));
    mostrarRespuesta(datos.mensaje || (r.ok ? "Orden enviada." : "CENTRAL ha rechazado la orden (HTTP " + r.status + ")."));
  } catch (err) {
    mostrarRespuesta("No se ha podido enviar la orden: CENTRAL no responde.");
  }
  actualizar();
}

function mostrarRespuesta(texto) {
  $("respuesta").textContent = texto;
}

// ---------------- Pintar la página ----------------
function pintar() {
  pintarResumen();
  pintarTabla();
  pintarPeticiones();
  pintarCentral();
  pintarDetalle();
  pintarBotones();
  pintarEventos();
}

// Recuento de estaciones por estado
function pintarResumen() {
  const lista = $("resumen");
  lista.replaceChildren();
  for (const estado of Object.keys(ETIQUETAS)) {
    const n = estaciones.filter(e => e.estado === estado).length;
    const li = crear("li", undefined, "estado-" + estado.toLowerCase());
    li.append(crear("strong", String(n)), crear("span", ETIQUETAS[estado]));
    lista.append(li);
  }
}

// Tabla de estaciones: ID, ubicación, estado, caudal, volumen y operario (pág. 4)
function pintarTabla() {
  const cuerpo = $("tabla");
  cuerpo.replaceChildren();
  $("tabla-vacia").hidden = estaciones.length > 0;

  const ordenadas = [...estaciones].sort((a, b) => a.id.localeCompare(b.id, "es", { numeric: true }));

  for (const e of ordenadas) {
    const fila = document.createElement("tr");
    fila.tabIndex = 0;
    if (e.id === seleccion) {
      fila.classList.add("seleccionada");
      fila.setAttribute("aria-selected", "true");
    }

    const regando = e.estado === "REGANDO";
    const sinDatos = e.estado === "DESCONECTADA";   // no se puede monitorizar (pág. 4)

    const celdaEstado = document.createElement("td");
    celdaEstado.append(etiquetaEstado(e.estado));

    fila.append(
      crear("td", e.id),
      crear("td", e.ubicacion || "—"),
      celdaEstado,
      crear("td", sinDatos ? "—" : formatear(regando ? e.caudal : 0), "numero"),
      crear("td", sinDatos ? "—" : formatear(regando ? e.volumen : 0), "numero"),
      crear("td", regando && e.operario ? e.operario : "—")
    );

    // Seleccionar con clic o con teclado
    fila.addEventListener("click", () => seleccionar(e.id));
    fila.addEventListener("keydown", (ev) => {
      if (ev.key === "Enter" || ev.key === " ") {
        ev.preventDefault();
        seleccionar(e.id);
      }
    });
    cuerpo.append(fila);
  }
}

// Peticiones de activación (pág. 4): primero las activas, después las terminadas
function pintarPeticiones() {
  const cuerpo = $("peticiones");
  cuerpo.replaceChildren();
  $("peticiones-vacia").hidden = peticiones.length > 0;

  const activas    = peticiones.filter(p => !FASES_FINALES.includes(p.fase)).reverse();
  const terminadas = peticiones.filter(p =>  FASES_FINALES.includes(p.fase)).reverse();

  for (const p of [...activas, ...terminadas]) {
    const fila = document.createElement("tr");
    if (FASES_FINALES.includes(p.fase)) fila.classList.add("terminada");

    const celdaFase = document.createElement("td");
    celdaFase.append(etiquetaFase(p.fase));

    fila.append(
      crear("td", p.hora || "—"),
      crear("td", p.operario || "—"),
      crear("td", p.ws || "—"),
      crear("td", p.duracion ? String(p.duracion) : "—", "numero"),
      celdaFase,
      crear("td", p.resultado || "—", "texto-suave")
    );
    cuerpo.append(fila);
  }
}

// Estado del sistema central (pág. 4)
function pintarCentral() {
  const dl = $("central");
  dl.replaceChildren();

  const conectadas  = estaciones.filter(e => e.estado !== "DESCONECTADA").length;
  const regando     = estaciones.filter(e => e.estado === "REGANDO").length;
  const enFuga      = estaciones.filter(e => e.estado === "FUGA").length;
  const activas     = peticiones.filter(p => !FASES_FINALES.includes(p.fase)).length;

  if (!conectado) {
    filaDatos(dl, "CENTRAL", "Sin respuesta", "error");
  } else {
    filaDatos(dl, "CENTRAL", "En funcionamiento", "ok");
  }

  if (central.kafka === true) {
    filaDatos(dl, "Gestor de colas (Kafka)", "Conectado", "ok");
  } else if (central.kafka === false) {
    filaDatos(dl, "Gestor de colas (Kafka)", "Sin conexión", "error");
  } else {
    filaDatos(dl, "Gestor de colas (Kafka)", "—");
  }

  filaDatos(dl, "En marcha desde", central.arranque || "—");
  filaDatos(dl, "Estaciones registradas", String(estaciones.length));
  filaDatos(dl, "Monitores conectados", conectadas + " de " + estaciones.length,
            conectadas < estaciones.length ? "alerta" : "ok");
  filaDatos(dl, "Riegos en curso", String(regando));
  filaDatos(dl, "Estaciones con fuga", String(enFuga), enFuga > 0 ? "error" : undefined);
  filaDatos(dl, "Peticiones activas", String(activas));
  filaDatos(dl, "Última actualización", ultimaActualizacion || "—");
}

function seleccionar(id) {
  seleccion = id;
  document.querySelector('input[name="destino"][value="una"]').checked = true;
  mostrarRespuesta("");
  pintar();
}

// Detalle de la estación seleccionada, con sus componentes (pág. 5; Figuras 1 y 2, pág. 6)
function pintarDetalle() {
  const caja = $("detalle");
  caja.replaceChildren();

  const e = estaciones.find(x => x.id === seleccion);
  if (!e) {
    caja.append(crear("p", "Selecciona una estación en la tabla para ver su detalle y enviarle órdenes.", "vacio"));
    return;
  }

  const regando  = e.estado === "REGANDO";
  const sinDatos = e.estado === "DESCONECTADA";
  const fuga     = e.estado === "FUGA";

  caja.append(
    crear("p", e.id, "titulo-ws"),
    crear("p", e.ubicacion || "Ubicación desconocida", "ubicacion")
  );

  // Datos del riego
  const dl = document.createElement("dl");
  const dd = document.createElement("dd");
  dd.append(etiquetaEstado(e.estado));
  dl.append(crear("dt", "Estado"), dd);
  filaDatos(dl, "Caudal",            sinDatos ? "—" : formatear(regando ? e.caudal : 0) + " L/min");
  filaDatos(dl, "Volumen acumulado", sinDatos ? "—" : formatear(regando ? e.volumen : 0) + " L");
  filaDatos(dl, "Operario",          regando && e.operario ? e.operario : "—");
  filaDatos(dl, "Último riego",      e.ultimoRiego || "—");
  caja.append(dl);

  // Componentes de la estación (pág. 5): caudalímetro, detector de fuga y electroválvula.
  // Se deducen del estado de la WS                                              [diseño]
  caja.append(crear("p", "Componentes", "subtitulo"));
  const comp = document.createElement("dl");

  if (sinDatos) {
    filaDatos(comp, "Caudalímetro",      "Sin datos");
    filaDatos(comp, "Detector de fuga",  "Sin datos");
    filaDatos(comp, "Electroválvula",    "Sin datos");
  } else {
    filaDatos(comp, "Caudalímetro",      regando ? "Midiendo" : "En espera", "ok");
    filaDatos(comp, "Detector de fuga",  fuga ? "Fuga detectada" : "Sin incidencias", fuga ? "error" : "ok");
    filaDatos(comp, "Electroválvula",    regando ? "Abierta" : "Cerrada", regando ? "ok" : undefined);
  }
  caja.append(comp);
}

// Desactiva las órdenes que no tienen sentido. Con "Todas" decide CENTRAL estación por estación
function pintarBotones() {
  const destino = document.querySelector('input[name="destino"]:checked').value;
  const e = estaciones.find(x => x.id === seleccion);
  const iniciar  = document.querySelector('[data-accion="INICIAR"]');
  const bloquear = document.querySelector('[data-accion="BLOQUEAR"]');
  const activar  = document.querySelector('[data-accion="ACTIVAR"]');

  if (!conectado) {
    iniciar.disabled = bloquear.disabled = activar.disabled = true;
    return;
  }
  if (destino === "todas") {
    iniciar.disabled = bloquear.disabled = activar.disabled = estaciones.length === 0;
    return;
  }
  if (!e) {
    iniciar.disabled = bloquear.disabled = activar.disabled = true;
    return;
  }
  iniciar.disabled  = e.estado !== "DISPONIBLE";                                   // punto 4, pág. 7
  bloquear.disabled = e.estado === "FUERA_DE_SERVICIO" || e.estado === "DESCONECTADA";
  activar.disabled  = e.estado !== "FUERA_DE_SERVICIO";
}

// Mensajes de la aplicación
function pintarEventos() {
  const lista = $("eventos");
  lista.replaceChildren();
  if (eventos.length === 0) {
    lista.append(crear("li", "Todavía no hay mensajes."));
    return;
  }
  for (const ev of eventos) {
    const li = document.createElement("li");
    li.append(crear("time", ev.hora), document.createTextNode(ev.texto));
    lista.append(li);
  }
}

// ---------------- Arranque ----------------
document.querySelectorAll("[data-accion]").forEach(b =>
  b.addEventListener("click", () => enviarOrden(b.dataset.accion))
);
document.querySelectorAll('input[name="destino"]').forEach(r =>
  r.addEventListener("change", pintarBotones)
);

actualizar();
setInterval(actualizar, PERIODO_MS);