const express = require("express");
const router = express.Router();
const { 
    uploadExcel, 
    listTables, 
    getTableData,
    tablasDisponibles,
    procesarParte1,
    procesarParte2,
    procesarParte3,
    procesarParte4,
    procesarParte5,
    copiarDatosPresupuestales,
    getCpcOptions,
    actualizarFila,
    getProductosMgaOptions
} = require("../controllers/cuipoController");

// Subir archivo Excel
router.post("/upload", uploadExcel);

// Listar tablas del esquema
router.get("/tables", listTables);

// Ver datos de una tabla específica
router.get("/tables/:tableName", getTableData);

// Ver las tablas disponibles en ejecucion
router.get("/ejecucion/obtener-tablas-disponibles", tablasDisponibles);

// Procesar la parte 1 - Fondo
router.post('/ejecucion/procesar/parte1', procesarParte1);

// Procesar la parte 2 - Centro Gestor
router.post('/ejecucion/procesar/parte2', procesarParte2);

// Procesar la parte 3 - Pospre
router.post('/ejecucion/procesar/parte3', procesarParte3);

// Procesar la parte 4 - Proyecto
router.post('/ejecucion/procesar/parte4', procesarParte4);

// Procesar la parte 5 - Area Funcional
router.post('/ejecucion/procesar/parte5', procesarParte5);

// Cargar datos de la tabla base a la plantilla
router.post('/ejecucion/copiar-datos-presupuestales', copiarDatosPresupuestales);

// Obtener las opciones del cpc
router.get('/ejecucion/cpc-options/:lastDigit', getCpcOptions);
router.post('/ejecucion/actualizar-fila', actualizarFila);

// Obtener producto MGA
router.get('/ejecucion/productos-mga-options', getProductosMgaOptions);


module.exports = router;