const multer = require("multer");
const path = require("path");
const XLSX = require("xlsx");
const fs = require("fs");
const { pool } = require("../db");
const estadisticasModel = require('../models/estadisticasModel');

// Configuración de multer
const storage = multer.diskStorage({
  destination: (req, file, cb) => {
    cb(null, "uploads/");
  },
  filename: (req, file, cb) => {
    cb(null, Date.now() + "-" + file.originalname);
  },
});

const upload = multer({
  storage,
  fileFilter: (req, file, cb) => {
    const ext = path.extname(file.originalname);
    if (ext !== ".xlsx" && ext !== ".xlsm") {
      return cb(new Error("Solo se permiten archivos Excel (.xlsx, .xlsm)"));
    }
    cb(null, true);
  },
}).single("file");

function normalizeName(name) {
  return name
    .toLowerCase()
    .replace(/\s+/g, "_")
    .replace(/[^a-z0-9_]/g, "");
}


// Controlador para subir y procesar el archivo Excel
async function uploadExcel(req, res) {
  upload(req, res, async function (err) {
    if (err) return res.status(400).json({ error: err.message });
    if (!req.file) return res.status(400).json({ error: "Archivo no recibido" });

    try {
      const filePath = req.file.path;
      const workbook = XLSX.readFile(filePath);
      const sheetName = workbook.SheetNames[0];
      const rawSheet = XLSX.utils.sheet_to_json(workbook.Sheets[sheetName], {
        defval: null,
        raw: false
      });

      const worksheet = rawSheet.filter(row =>
        Object.values(row).some(cell => cell !== null && cell !== "")
      );

      if (worksheet.length === 0) {
        fs.unlinkSync(filePath);
        return res.status(400).json({ error: "El archivo está vacío o mal formado" });
      }

      const tableName = normalizeName(path.parse(req.file.originalname).name);
      const rawHeaders = Object.keys(worksheet[0]).filter(
        h => h && !/^__empty/i.test(h.trim())
      );
      const normalizedHeaders = rawHeaders.map(normalizeName);

      // 🔥 Eliminar tabla si ya existe
      const dropQuery = `DROP TABLE IF EXISTS ${process.env.DB_SCHEMA}."${tableName}";`;
      await pool.query(dropQuery);

      // Crear tabla con ID SERIAL desde cero
      const createTableQuery = `
        CREATE TABLE ${process.env.DB_SCHEMA}."${tableName}" (
          id SERIAL PRIMARY KEY,
          ${normalizedHeaders.map(h => `"${h}" TEXT`).join(", ")}
        );
      `;
      await pool.query(createTableQuery);

      // Insertar datos
      for (let row of worksheet) {
        const values = rawHeaders.map(h => row[h] ?? "").map(val => val.toString().trim());
        const isEmptyRow = values.every(v => v === "");
        if (isEmptyRow) continue;

        const insertQuery = `
          INSERT INTO ${process.env.DB_SCHEMA}."${tableName}" (${normalizedHeaders.map(h => `"${h}"`).join(", ")})
          VALUES (${normalizedHeaders.map((_, i) => `$${i + 1}`).join(", ")});
        `;
        await pool.query(insertQuery, values);
      }

      const previewQuery = `SELECT * FROM ${process.env.DB_SCHEMA}."${tableName}" LIMIT 10;`;
      const preview = await pool.query(previewQuery);

      fs.unlinkSync(filePath);

      await pool.query(`
        SELECT setval(
          pg_get_serial_sequence('${process.env.DB_SCHEMA}."${tableName}"', 'id'),
          1,
          false
        );
      `);

      return res.status(200).json({
        message: `Archivo ${req.file.originalname} subido correctamente`,
        table: tableName,
        preview: preview.rows,
      });
    } catch (e) {
      console.error("❌ Error al procesar el archivo:", e.message);
      return res.status(500).json({ error: "Error al procesar el archivo" });
    }
  });
}

// Controlador para listar todas las tablas en el esquema sis_catastro_verificacion
async function listTables(req, res) {
  try {
    const result = await pool.query(`
      SELECT table_name
      FROM information_schema.tables
      WHERE table_schema = $1
        AND table_type = 'BASE TABLE';
    `, [process.env.DB_SCHEMA]);

    const tables = result.rows.map(row => row.table_name);

    return res.status(200).json({ tables });
  } catch (error) {
    console.error("❌ Error al listar tablas:", error.message);
    return res.status(500).json({ error: "Error al obtener las tablas" });
  }
}

// Controlador para obtener los datos de una tabla específica
async function getTableData(req, res) {
  const { tableName } = req.params;

  const userRole = req.user?.role || req.user?.roleName;
  const userDependencyId = req.user?.id_dependency_user;

  console.log("📥 Tabla solicitada:", tableName);
  console.log("🔑 Rol del usuario:", userRole);
  console.log("🏢 ID de dependencia del usuario:", userDependencyId);

  if (!tableName) {
    return res.status(400).json({ error: "Nombre de tabla no proporcionado" });
  }

  try {
    let querySQL = '';
    let whereClause = '';

    // Si el usuario tiene rol 'user' o 'querry' y accede a la tabla restringida
    if (
      ["user", "querry"].includes(userRole?.toLowerCase()) &&
      tableName === "cuipo_plantilla_distrito_2025_vf" &&
      userDependencyId
    ) {
      console.log("🔒 Aplicando filtro por dependencia para el usuario...");

      const dependencyQuery = await pool.query(
        `SELECT dependency_name FROM tbl_dependency WHERE id_dependency = $1`,
        [userDependencyId]
      );

      if (dependencyQuery.rows.length > 0) {
        const dependencyName = dependencyQuery.rows[0].dependency_name;
        const cleanedDependencyName = dependencyName.replace(/'/g, "''");

        console.log("✅ Nombre de dependencia encontrada:", dependencyName);

        whereClause = `WHERE secretaria = '${cleanedDependencyName}'`;
      } else {
        console.warn(`⚠️ No se encontró dependency_name para id: ${userDependencyId}`);
        return res.status(200).json({ rows: [] });
      }
    }

    // Construir consulta SQL final
    if (tableName === "cuipo_plantilla_distrito_2025_vf") {
      querySQL = `
        SELECT *
        FROM ${process.env.DB_SCHEMA}."${tableName}"
        ${whereClause}
        ORDER BY
          CASE WHEN fondo = 'Totales' THEN 1 ELSE 0 END,
          id ASC;
      `;
    } else {
      querySQL = `SELECT * FROM ${process.env.DB_SCHEMA}."${tableName}";`;
    }

    console.log("🧠 Ejecutando query SQL:\n", querySQL);

    const result = await pool.query(querySQL);

    return res.status(200).json({ rows: result.rows });
  } catch (error) {
    console.error(`❌ Error al obtener datos de la tabla ${tableName}:`, error.message);
    return res.status(500).json({ error: `Error al obtener los datos de la tabla ${tableName}` });
  }
}

// CONTROLADOR PARA OBTENER LAS TABLAS DISPONIBLES PARA EJECUCION
async function tablasDisponibles(req, res) {
  try {
    console.log('📌 Entró a controlador tablasDisponibles');
    const queryTablas = `
      SELECT table_name 
      FROM information_schema.tables 
      WHERE table_schema = 'sis_catastro_verificacion'
      AND (table_name = 'cuipo_plantilla_distrito_2025_vf' 
           OR table_name = 'base_de_ejecucion_presupuestal_30062025'
           OR table_name LIKE 'cuipo2%')
      ORDER BY table_name;
    `;

    const resultTablas = await pool.query(queryTablas);
    const tablas = resultTablas.rows.map(row => row.table_name);

    const { tabla } = req.query;
    let datosTabla = null;

    if (tabla && tablas.includes(tabla)) {
      let queryDatos;

      // Aplica filtro por secretaría si el usuario tiene rol "user"
      if (
        req.user &&
        req.user.role === 'user' &&
        tabla === 'cuipo_plantilla_distrito_2025_vf'
      ) {
        const secretaria = req.user.dependencyName;

        queryDatos = `
          SELECT *
          FROM sis_catastro_verificacion.${tabla}
          WHERE secretaria = $1
        `;
        const resultDatos = await pool.query(queryDatos, [secretaria]);
        datosTabla = resultDatos.rows;

      } else {
        // Sin filtro
        queryDatos = `SELECT * FROM sis_catastro_verificacion.${tabla};`;
        const resultDatos = await pool.query(queryDatos);
        datosTabla = resultDatos.rows;
      }
    }
    console.log('✅ tablas disponibles que se envían:', tablas);
    return res.status(200).json({
      success: true,
      tablasDisponibles: tablas,
      datosTabla: datosTabla,
      tablaSeleccionada: tabla || null
    });

  } catch (error) {
    console.error('Error en tablasDisponibles:', error);
    return res.status(500).json({ 
      success: false,
      error: 'Error al obtener tablas',
      details: error.message 
    });
  }
}

// CONTROLADOR PARA LA PARTE 1 - FONDO
async function procesarParte1(req, res) {
  const client = await pool.connect();

  try {
    await client.query('BEGIN'); // Iniciar una transacción

    console.log("Iniciando procesamiento de Parte 1...");

    // 1. Validar existencia de todas las tablas necesarias
    const tablasAValidar = [
      { schema: process.env.DB_SCHEMA, name: 'base_de_ejecucion_presupuestal_30062025' },
      { schema: process.env.DB_SCHEMA, name: 'cuipo_plantilla_distrito_2025_vf' },
      { schema: process.env.DB_SCHEMA, name: 'fuentes_cuipo' }
    ];

    for (const tablaInfo of tablasAValidar) {
      const check = await client.query(
        `SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = $1 AND table_schema = $2)`,
        [tablaInfo.name, tablaInfo.schema]
      );
      if (!check.rows[0].exists) {
        throw new Error(`Tabla "${tablaInfo.schema}.${tablaInfo.name}" no existe en la base de datos o en el esquema.`);
      }
    }

    // --- PRIMER UPDATE: Calcular 'fuente' y 'vigencia_gasto' ---
    const update1Query = `
      UPDATE ${process.env.DB_SCHEMA}."cuipo_plantilla_distrito_2025_vf" AS dest
      SET
          fuente = CASE
                       WHEN dest.fondo = 'Totales' THEN NULL
                       WHEN LENGTH(dest.fondo) >= 9 AND SUBSTRING(dest.fondo FROM 5 FOR 5) ~ '^[0-9]{5}$'
                       THEN SUBSTRING(dest.fondo FROM 5 FOR 5)
                       ELSE NULL
                   END,
          vigencia_gasto = CASE
                                WHEN dest.fondo = 'Totales' THEN NULL
                                WHEN LEFT(dest.fondo, 2) ~ '^[0-9]+$' AND LEFT(dest.fondo, 2)::numeric = 16 THEN '2'
                                ELSE '1'
                            END
      WHERE dest.fondo IS NOT NULL OR dest.fondo = 'Totales'; -- Asegura que procesamos todas las filas relevantes

    `;
    console.log("Ejecutando Primer UPDATE para 'fuente' y 'vigencia_gasto'...");
    const result1 = await client.query(update1Query);
    console.log(`Primer UPDATE completado. Registros actualizados: ${result1.rowCount}`);

    // --- SEGUNDO UPDATE: Calcular 'fuente_cuipo' y 'situacion_de_fondos' ---
    // Este UPDATE se basa en los valores de 'fuente' que acaban de ser calculados y guardados.
    const update2Query = `
      UPDATE ${process.env.DB_SCHEMA}."cuipo_plantilla_distrito_2025_vf" AS dest
      SET
          fuente_cuipo = COALESCE(fc.cod_cuipo, ''),
          situacion_de_fondos = COALESCE(fc.situacion_de_fondos, '')
      FROM ${process.env.DB_SCHEMA}.fuentes_cuipo AS fc
      WHERE TRIM(dest.fuente) = TRIM(fc.cod)
      AND dest.fuente IS NOT NULL; -- Solo intentar actualizar si 'fuente' no es NULL
    `;
    console.log("Ejecutando Segundo UPDATE para 'fuente_cuipo' y 'situacion_de_fondos'...");
    const result2 = await client.query(update2Query);
    console.log(`Segundo UPDATE completado. Registros actualizados: ${result2.rowCount}`);

    await client.query('COMMIT'); // Confirmar la transacción

    console.log(`Parte 1 completada. Total registros afectados: ${result1.rowCount + result2.rowCount}`);

    return res.status(200).json({
      success: true,
      message: "Parte 1 procesada exitosamente: Campos de fondo, fuente, fuente_cuipo, vigencia_gasto y situación_de_fondos actualizados.",
      registros_afectados_paso1: result1.rowCount,
      registros_afectados_paso2: result2.rowCount,
      detalles: {
        acciones_realizadas: [
          "Primer paso: Cálculo y validación del campo 'fuente' y 'vigencia_gasto'.",
          "Segundo paso: Cálculo del campo 'fuente_cuipo' y 'situacion_de_fondos' (búsqueda en 'fuentes_cuipo' por 'fuente')."
        ]
      }
    });

  } catch (error) {
    await client.query('ROLLBACK'); // Revertir la transacción en caso de error
    console.error('❌ Error en procesarParte1:', error.message);

    let userMessage = "Error desconocido al procesar Parte 1.";
    if (error.message.includes("no existe en la base de datos o en el esquema")) {
      userMessage = `Una de las tablas necesarias no fue encontrada: ${error.message}`;
    } else if (error.message.includes("column") && error.message.includes("does not exist")) {
      userMessage = `Error de columna. Posiblemente un nombre de columna incorrecto en la base de datos: ${error.message}`;
    } else {
      userMessage = error.message;
    }

    return res.status(500).json({
      success: false,
      error: "Error al procesar Parte 1",
      detalles: userMessage,
      solucion_sugerida: [
        `Verifique que las tablas ${tablasAValidar.map(t => `${t.schema}.${t.name}`).join(', ')} existan.`,
        "Asegúrese de que los nombres de las columnas ('fondo', 'fuente', 'cod', 'cod_cuipo', 'situacion_de_fondos', 'vigencia_gasto') sean correctos en todas las tablas involucradas.",
        "Confirme que los tipos de datos de las columnas son compatibles con las operaciones realizadas (ej. SUBSTRING, LEFT, numeric, expresiones regulares)."
      ]
    });
  } finally {
    client.release(); // Liberar la conexión al pool
  }
}

// CONTROLADOR PARA LA PARTE 2 - CENTRO GESTOR
async function procesarParte2(req, res) {
    const client = await pool.connect(); 

    try {
        await client.query('BEGIN'); 

        console.log("Iniciando procesamiento de Parte 2: Centro Gestor y Dependencias...");

        // 1. Validar existencia de todas las tablas necesarias (Mantener esto, es buena práctica)
        const tablasAValidar = [
            { schema: process.env.DB_SCHEMA, name: 'cuipo_plantilla_distrito_2025_vf' },
            { schema: process.env.DB_SCHEMA, name: 'dependencias' },
            { schema: process.env.DB_SCHEMA, name: 'terceros' },
            { schema: process.env.DB_SCHEMA, name: 'estapublicos' },
            { schema: process.env.DB_SCHEMA, name: 'base_de_ejecucion_presupuestal_30062025' } 
        ];

        for (const tablaInfo of tablasAValidar) {
            const check = await client.query(
                `SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = $1 AND table_schema = $2)`,
                [tablaInfo.name, tablaInfo.schema]
            );
            if (!check.rows[0].exists) {
                throw new Error(`Tabla "${tablaInfo.schema}.${tablaInfo.name}" no existe en la base de datos o en el esquema.`);
            }
        }

        // --- ¡NUEVA LÓGICA AÑADIDA / MODIFICADA! ---
        // Paso 1: Copiar 'centro_gestor' y 'proyecto' desde la tabla base a la plantilla
        // Asumo que 'base_de_ejecucion_presupuestal_30062025' contiene los campos 'centro_gestor' y 'proyecto'
        // que necesitan ser transferidos a 'cuipo_plantilla_distrito_2025_vf'.
        // Si 'proyecto' también se llena en Parte 2, inclúyelo aquí también.

        console.log("Copiando 'centro_gestor' y 'proyecto' desde base_de_ejecucion_presupuestal_30062025 a cuipo_plantilla_distrito_2025_vf...");
        const copyCentroGestorProyectoQuery = `
            UPDATE ${process.env.DB_SCHEMA}."cuipo_plantilla_distrito_2025_vf" AS dest
            SET
                centro_gestor = base.centro_gestor,
                proyecto = base.proyecto -- Asegúrate de copiar 'proyecto' también si es necesario aquí
            FROM
                ${process.env.DB_SCHEMA}."base_de_ejecucion_presupuestal_30062025" AS base
            WHERE
                dest.id = base.id
                AND (dest.centro_gestor IS DISTINCT FROM base.centro_gestor OR dest.proyecto IS DISTINCT FROM base.proyecto); -- Solo actualizar si hay cambios
        `;
        const copyResult = await client.query(copyCentroGestorProyectoQuery);
        console.log(`Copia de 'centro_gestor' y 'proyecto' completada. Registros actualizados: ${copyResult.rowCount}`);

        // --- FIN DE LA LÓGICA AÑADIDA / MODIFICADA ---

        // --- UPDATE PRINCIPAL: Calcular 'seccion_ptal_cuipo', 'secretaria' y 'tercero_cuipo' ---
        const updateQuery = `
            WITH calculated_values AS (
                SELECT
                    cpf.id,
                    cpf.centro_gestor,
                    cpf.proyecto, 

                    -- Formula 1: seccion_ptal_cuipo
                    COALESCE(
                        (SELECT TRIM(d.seccion_presupuestal)
                           FROM ${process.env.DB_SCHEMA}.dependencias AS d
                           WHERE TRIM(cpf.centro_gestor) = TRIM(d.centro_gestor)
                           LIMIT 1),
                        '' 
                    ) AS new_seccion_ptal_cuipo,

                    -- Tercera formula: secretaria (calculada antes porque 'tercero_cuipo' depende de ella)
                    CASE
                        WHEN SUBSTRING(TRIM(cpf.centro_gestor), 1, 3) = '704' THEN
                             COALESCE(
                                 (SELECT TRIM(ep.establecimiento_publico)
                                   FROM ${process.env.DB_SCHEMA}.estapublicos AS ep
                                   WHERE TRIM(cpf.proyecto) = TRIM(ep.proyecto)
                                   LIMIT 1),
                                 'SECRETARÍA DE HACIENDA'
                             )
                        ELSE
                             COALESCE(
                                 (SELECT TRIM(d2.dependencia)
                                   FROM ${process.env.DB_SCHEMA}.dependencias AS d2
                                   WHERE TRIM(cpf.centro_gestor) = TRIM(d2.centro_gestor)
                                   LIMIT 1),
                                 'SECRETARÍA DE HACIENDA'
                             )
                    END AS new_secretaria
                FROM
                    ${process.env.DB_SCHEMA}."cuipo_plantilla_distrito_2025_vf" AS cpf
                WHERE
                    cpf.centro_gestor IS NOT NULL AND TRIM(cpf.centro_gestor) != '' -- Esta condición AHORA sí debería encontrar datos
            ),
            final_calculated_values AS (
                SELECT
                    cv.id,
                    cv.new_seccion_ptal_cuipo,
                    cv.new_secretaria,
                    -- Segunda formula: tercero_cuipo (depende de new_secretaria)
                    COALESCE(
                        (SELECT TRIM(t.codigo)
                           FROM ${process.env.DB_SCHEMA}.terceros AS t
                           WHERE TRIM(cv.new_secretaria) = TRIM(t.establecimientos_publicos)
                           LIMIT 1),
                        '1'
                    ) AS new_tercero_cuipo
                FROM
                    calculated_values AS cv
            )
            UPDATE ${process.env.DB_SCHEMA}."cuipo_plantilla_distrito_2025_vf" AS dest
            SET
                seccion_ptal_cuipo = fcv.new_seccion_ptal_cuipo,
                secretaria = fcv.new_secretaria,
                tercero_cuipo = fcv.new_tercero_cuipo
            FROM final_calculated_values AS fcv
            WHERE dest.id = fcv.id;
        `;

        console.log("Ejecutando UPDATE para 'seccion_ptal_cuipo', 'secretaria' y 'tercero_cuipo'...");
        const resultUpdates = await client.query(updateQuery);
        console.log(`UPDATE de Parte 2 completado. Registros actualizados: ${resultUpdates.rowCount}`);

        await client.query('COMMIT'); 

        console.log(`Parte 2 completada. Total registros afectados: ${resultUpdates.rowCount}`);

        return res.status(200).json({
            success: true,
            message: "Parte 2 procesada exitosamente: Campos de centro_gestor, seccion_ptal_cuipo, tercero_cuipo y secretaria actualizados.",
            registros_afectados: resultUpdates.rowCount,
            detalles: {
                acciones_realizadas: [
                    "Copia inicial de 'centro_gestor' y 'proyecto' a la plantilla.", // Añadido
                    "Cálculo del campo 'seccion_ptal_cuipo' (basado en 'centro_gestor' y la tabla 'dependencias').",
                    "Cálculo del campo 'secretaria' (lógica condicional basada en 'centro_gestor', 'proyecto', y las tablas 'estapublicos'/'dependencias').",
                    "Cálculo del campo 'tercero_cuipo' (basado en 'secretaria' y la tabla 'terceros', con valor por defecto '1')."
                ]
            }
        });

    } catch (error) {
        await client.query('ROLLBACK');
        console.error('❌ Error en procesarParte2:', error.message);

        let userMessage = "Error desconocido al procesar Parte 2.";
        if (error.message.includes("no existe en la base de datos o en el esquema")) {
            userMessage = `Una de las tablas necesarias no fue encontrada: ${error.message}`;
        } else if (error.message.includes("column") && error.message.includes("does not exist")) {
            userMessage = `Error de columna. Posiblemente un nombre de columna incorrecto en la base de datos: ${error.message}`;
        } else {
            userMessage = error.message;
        }

        return res.status(500).json({
            success: false,
            error: "Error al procesar Parte 2",
            detalles: userMessage,
            solucion_sugerida: [
                `Verifique que las tablas ${tablasAValidar.map(t => `${t.schema}.${t.name}`).join(', ')} existan.`,
                "Asegúrese de que los nombres de las columnas ('centro_gestor', 'proyecto', 'seccion_ptal_cuipo', 'secretaria', 'tercero_cuipo', 'establecimientos_publicos', 'codigo', 'seccion_presupuestal', 'dependencia') sean correctos en todas las tablas involucradas.",
                "Confirme que los tipos de datos de las columnas son compatibles con las operaciones realizadas."
            ]
        });
    } finally {
        if (client) {
            client.release(); 
        }
    }
};

// CONTROLADOR PARA LA PARTE 3 - POSPRE
async function procesarParte3(req, res) {
    const client = await pool.connect(); 

    try {
        await client.query('BEGIN'); 

        console.log("Iniciando procesamiento de Parte 3: POSPRE...");

        const tablasAValidar = [
            { schema: process.env.DB_SCHEMA, name: 'cuipo_plantilla_distrito_2025_vf' },
            { schema: process.env.DB_SCHEMA, name: 'pospre_con_cpc_y_listas' },
            { schema: process.env.DB_SCHEMA, name: 'base_de_ejecucion_presupuestal_30062025' } 
        ];

        for (const tablaInfo of tablasAValidar) {
            const check = await client.query(
                `SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = $1 AND table_schema = $2)`,
                [tablaInfo.name, tablaInfo.schema]
            );
            if (!check.rows[0].exists) {
                throw new Error(`Tabla "${tablaInfo.schema}.${tablaInfo.name}" no existe en la base de datos o en el esquema.`);
            }
        }

        // --- ¡LÓGICA CORREGIDA PARA COPIAR 'posicion_presupuestaria' A 'pospre'! ---
        // Paso 1: Copiar 'posicion_presupuestaria' desde la tabla base a 'pospre' en la plantilla
        console.log("Copiando 'posicion_presupuestaria' desde base_de_ejecucion_presupuestal_30062025 a 'pospre' en cuipo_plantilla_distrito_2025_vf...");
        const copyPospreQuery = `
            UPDATE ${process.env.DB_SCHEMA}."cuipo_plantilla_distrito_2025_vf" AS dest
            SET
                pospre = base.posicion_presupuestaria -- Usamos 'posicion_presupuestaria' del origen
            FROM
                ${process.env.DB_SCHEMA}."base_de_ejecucion_presupuestal_30062025" AS base
            WHERE
                dest.id = base.id
                AND (dest.pospre IS DISTINCT FROM base.posicion_presupuestaria); -- Solo actualizar si hay cambios
        `;
        const copyResult = await client.query(copyPospreQuery);
        console.log(`Copia de 'pospre' completada. Registros actualizados: ${copyResult.rowCount}`);
        // --- FIN DE LA LÓGICA CORREGIDA ---

        // --- Primer UPDATE: Calcular 'validacion_pospre' y 'pospre_cuipo' ---
        const updateQuery = `
            WITH updated_pospre_data AS (
                SELECT
                    cpf.id,
                    -- Lógica para 'validacion_pospre' y 'pospre_cuipo'
                    COALESCE(
                        (SELECT TRIM(cpc.pospre_cuipo)
                           FROM ${process.env.DB_SCHEMA}.pospre_con_cpc_y_listas AS cpc
                           WHERE TRIM(cpf.pospre) = TRIM(cpc.pospre)
                           LIMIT 1),
                        (SELECT TRIM(cpc.pospre_cuipo)
                           FROM ${process.env.DB_SCHEMA}.pospre_con_cpc_y_listas AS cpc
                           WHERE LENGTH(TRIM(cpf.pospre)) >= 3 AND TRIM(LEFT(cpf.pospre, LENGTH(cpf.pospre) - 2)) = TRIM(cpc.pospre)
                           LIMIT 1),
                        '' 
                    ) AS new_pospre_cuipo_and_validacion
                FROM
                    ${process.env.DB_SCHEMA}."cuipo_plantilla_distrito_2025_vf" AS cpf
                WHERE
                    cpf.pospre IS NOT NULL AND TRIM(cpf.pospre) != '' 
            )
            UPDATE ${process.env.DB_SCHEMA}."cuipo_plantilla_distrito_2025_vf" AS dest
            SET
                validacion_pospre = upd.new_pospre_cuipo_and_validacion,
                pospre_cuipo = upd.new_pospre_cuipo_and_validacion
            FROM updated_pospre_data AS upd
            WHERE dest.id = upd.id;
        `;

        console.log("Ejecutando UPDATE para 'validacion_pospre' y 'pospre_cuipo'...");
        const resultPospreUpdates = await client.query(updateQuery);
        console.log(`UPDATE de POSPRE completado. Registros actualizados: ${resultPospreUpdates.rowCount}`);

        // --- Segundo UPDATE: Calcular 'tiene_cpc' ---
        const updateCpcQuery = `
            WITH updated_cpc_data AS (
                SELECT
                    cpf.id,
                    COALESCE(
                        (SELECT TRIM(cpc_list.pospre_cuipo)
                           FROM ${process.env.DB_SCHEMA}.pospre_con_cpc_y_listas AS cpc_list
                           WHERE TRIM(cpf.pospre_cuipo) = TRIM(cpc_list.pospre_cuipo)
                           LIMIT 1),
                        'NO APLICA' 
                    ) AS new_tiene_cpc
                FROM
                    ${process.env.DB_SCHEMA}."cuipo_plantilla_distrito_2025_vf" AS cpf
                WHERE
                    cpf.pospre_cuipo IS NOT NULL AND TRIM(cpf.pospre_cuipo) != '' 
            )
            UPDATE ${process.env.DB_SCHEMA}."cuipo_plantilla_distrito_2025_vf" AS dest
            SET
                tiene_cpc = upd.new_tiene_cpc
            FROM updated_cpc_data AS upd
            WHERE dest.id = upd.id;
        `;

        console.log("Ejecutando UPDATE para 'tiene_cpc'...");
        const resultCpcUpdate = await client.query(updateCpcQuery);
        console.log(`UPDATE de CPC completado. Registros actualizados: ${resultCpcUpdate.rowCount}`);

        await client.query('COMMIT');

        console.log(`Parte 3 completada. Total registros afectados: ${resultPospreUpdates.rowCount + resultCpcUpdate.rowCount}`);

        return res.status(200).json({
            success: true,
            message: "Parte 3 procesada exitosamente: Campos de pospre, validacion_pospre, pospre_cuipo y tiene_cpc actualizados.",
            registros_afectados_pospre: resultPospreUpdates.rowCount,
            registros_afectados_cpc: resultCpcUpdate.rowCount,
            detalles: {
                acciones_realizadas: [
                    "Copia inicial del campo 'pospre' a la plantilla desde 'posicion_presupuestaria'.",
                    "Cálculo del campo 'validacion_pospre' y 'pospre_cuipo' (basado en 'pospre' y la tabla 'pospre_con_cpc_y_listas').",
                    "Cálculo del campo 'tiene_cpc' (basado en 'pospre_cuipo' y la tabla 'pospre_con_cpc_y_listas')."
                ]
            }
        });

    } catch (error) {
        await client.query('ROLLBACK');
        console.error('❌ Error en procesarParte3:', error.message);

        let userMessage = "Error desconocido al procesar Parte 3.";
        if (error.message.includes("no existe en la base de datos o en el esquema")) {
            userMessage = `Una de las tablas necesarias no fue encontrada: ${error.message}`;
        } else if (error.message.includes("column") && error.message.includes("does not exist")) {
            userMessage = `Error de columna. Posiblemente un nombre de columna incorrecto en la base de datos: ${error.message}`;
        } else {
            userMessage = error.message;
        }

        return res.status(500).json({
            success: false,
            error: "Error al procesar Parte 3",
            detalles: userMessage,
            solucion_sugerida: [
                `Verifique que las tablas ${tablasAValidar.map(t => `${t.schema}.${t.name}`).join(', ')} existan.`,
                "Asegúrese de que los nombres de las columnas ('pospre', 'posicion_presupuestaria', 'pospre_cuipo') sean correctos en todas las tablas involucradas.",
                "Confirme que los tipos de datos de las columnas son compatibles con las operaciones realizadas."
            ]
        });
    } finally {
        if (client) {
            client.release(); 
        }
    }
}

// CONTROLADOR PARA LA PARTE 4 - PROYECTO
async function procesarParte4(req, res) {
    const client = await pool.connect(); 

    try {
        await client.query('BEGIN'); 

        console.log("Iniciando procesamiento de Parte 4: Proyecto...");

        const tablasAValidar = [
            { schema: process.env.DB_SCHEMA, name: 'cuipo_plantilla_distrito_2025_vf' },
            { schema: process.env.DB_SCHEMA, name: 'proyectos' },
            { schema: process.env.DB_SCHEMA, name: 'base_de_ejecucion_presupuestal_30062025' } // Asegurarse de que la tabla base esté validada aquí
        ];

        for (const tablaInfo of tablasAValidar) {
            const check = await client.query(
                `SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = $1 AND table_schema = $2)`,
                [tablaInfo.name, tablaInfo.schema]
            );
            if (!check.rows[0].exists) {
                throw new Error(`Tabla "${tablaInfo.schema}.${tablaInfo.name}" no existe en la base de datos o en el esquema.`);
            }
        }

        // --- ¡NUEVA LÓGICA AÑADIDA! ---
        // Paso 1: Copiar 'proyecto' desde la tabla base a 'proyecto' en la plantilla
        console.log("Copiando 'proyecto' desde base_de_ejecucion_presupuestal_30062025 a 'proyecto' en cuipo_plantilla_distrito_2025_vf...");
        const copyProyectoQuery = `
            UPDATE ${process.env.DB_SCHEMA}."cuipo_plantilla_distrito_2025_vf" AS dest
            SET
                proyecto = base.proyecto -- Asume que el campo se llama 'proyecto' en la tabla base también
            FROM
                ${process.env.DB_SCHEMA}."base_de_ejecucion_presupuestal_30062025" AS base
            WHERE
                dest.id = base.id
                AND (dest.proyecto IS DISTINCT FROM base.proyecto); -- Solo actualizar si hay cambios
        `;
        const copyResult = await client.query(copyProyectoQuery);
        console.log(`Copia de 'proyecto' completada. Registros actualizados: ${copyResult.rowCount}`);
        // --- FIN DE LA LÓGICA AÑADIDA ---

        // --- UPDATE: Calcular 'bpin' y 'nombre_proyecto' ---
        const updateQuery = `
            WITH updated_project_data AS (
                SELECT
                    cpf.id,
                    -- Lógica para 'bpin': Buscar en proyectos.p y traer distrito_m1
                    COALESCE(
                        (SELECT TRIM(proj.distrito_m1)
                           FROM ${process.env.DB_SCHEMA}.proyectos AS proj
                           WHERE TRIM(cpf.proyecto) = TRIM(proj.p)
                           LIMIT 1),
                        '' 
                    ) AS new_bpin,
                    -- Lógica para 'nombre_proyecto': Buscar en proyectos.p y traer nombre_proyecto
                    COALESCE(
                        (SELECT TRIM(proj.nombre_proyecto)
                           FROM ${process.env.DB_SCHEMA}.proyectos AS proj
                           WHERE TRIM(cpf.proyecto) = TRIM(proj.p)
                           LIMIT 1),
                        '' 
                    ) AS new_nombre_proyecto
                FROM
                    ${process.env.DB_SCHEMA}."cuipo_plantilla_distrito_2025_vf" AS cpf
                WHERE
                    cpf.proyecto IS NOT NULL AND TRIM(cpf.proyecto) != '' -- Esta condición AHORA sí debería encontrar datos
            )
            UPDATE ${process.env.DB_SCHEMA}."cuipo_plantilla_distrito_2025_vf" AS dest
            SET
                bpin = upd.new_bpin,
                nombre_proyecto = upd.new_nombre_proyecto
            FROM updated_project_data AS upd
            WHERE dest.id = upd.id;
        `;

        console.log("Ejecutando UPDATE para 'bpin' y 'nombre_proyecto'...");
        const resultUpdates = await client.query(updateQuery);
        console.log(`UPDATE de PROYECTO completado. Registros actualizados: ${resultUpdates.rowCount}`);

        await client.query('COMMIT'); 

        console.log(`Parte 4 completada. Total registros afectados: ${resultUpdates.rowCount}`);

        return res.status(200).json({
            success: true,
            message: "Parte 4 procesada exitosamente: Campos de proyecto, bpin y nombre_proyecto actualizados.",
            registros_afectados: resultUpdates.rowCount,
            detalles: {
                acciones_realizadas: [
                    "Copia inicial del campo 'proyecto' a la plantilla.",
                    "Cálculo del campo 'bpin' (basado en 'proyecto' y la tabla 'proyectos').",
                    "Cálculo del campo 'nombre_proyecto' (basado en 'proyecto' y la tabla 'proyectos')."
                ]
            }
        });

    } catch (error) {
        await client.query('ROLLBACK');
        console.error('❌ Error en procesarParte4:', error.message);

        let userMessage = "Error desconocido al procesar Parte 4.";
        if (error.message.includes("no existe en la base de datos o en el esquema")) {
            userMessage = `Una de las tablas necesarias no fue encontrada: ${error.message}`;
        } else if (error.message.includes("column") && error.message.includes("does not exist")) {
            userMessage = `Error de columna. Posiblemente un nombre de columna incorrecto en la base de datos: ${error.message}`;
        } else {
            userMessage = error.message;
        }

        return res.status(500).json({
            success: false,
            error: "Error al procesar Parte 4",
            detalles: userMessage,
            solucion_sugerida: [
                `Verifique que las tablas ${tablasAValidar.map(t => `${t.schema}.${t.name}`).join(', ')} existan.`,
                "Asegúrese de que los nombres de las columnas ('proyecto', 'p', 'distrito_m1', 'nombre_proyecto') sean correctos en todas las tablas involucradas.",
                "Confirme que los tipos de datos de las columnas son compatibles con las operaciones realizadas."
            ]
        });
    } finally {
        if (client) {
            client.release(); 
        }
    }
}

// CONTROLADOR PARA LA PARTE 5 - AREA FUNCIONAL
async function procesarParte5(req, res) {
    const client = await pool.connect(); 

    try {
        await client.query('BEGIN'); 

        console.log("Iniciando procesamiento de Parte 5: Área Funcional y Productos...");

        const tablasAValidar = [
            { schema: process.env.DB_SCHEMA, name: 'cuipo_plantilla_distrito_2025_vf' },
            { schema: process.env.DB_SCHEMA, name: 'base_de_ejecucion_presupuestal_30062025' } // Asegurarse de que la tabla base esté validada aquí
        ];

        for (const tablaInfo of tablasAValidar) {
            const check = await client.query(
                `SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = $1 AND table_schema = $2)`,
                [tablaInfo.name, tablaInfo.schema]
            );
            if (!check.rows[0].exists) {
                throw new Error(`Tabla "${tablaInfo.schema}.${tablaInfo.name}" no existe en la base de datos o en el esquema.`);
            }
        }

        // --- ¡NUEVA LÓGICA AÑADIDA! ---
        // Paso 1: Copiar 'area_funcional' desde la tabla base a 'area_funcional' en la plantilla
        console.log("Copiando 'area_funcional' desde base_de_ejecucion_presupuestal_30062025 a 'area_funcional' en cuipo_plantilla_distrito_2025_vf...");
        const copyAreaFuncionalQuery = `
            UPDATE ${process.env.DB_SCHEMA}."cuipo_plantilla_distrito_2025_vf" AS dest
            SET
                area_funcional = base.area_funcional -- Asume que el campo se llama 'area_funcional' en la tabla base también
            FROM
                ${process.env.DB_SCHEMA}."base_de_ejecucion_presupuestal_30062025" AS base
            WHERE
                dest.id = base.id
                AND (dest.area_funcional IS DISTINCT FROM base.area_funcional); -- Solo actualizar si hay cambios
        `;
        const copyResult = await client.query(copyAreaFuncionalQuery);
        console.log(`Copia de 'area_funcional' completada. Registros actualizados: ${copyResult.rowCount}`);
        // --- FIN DE LA LÓGICA AÑADIDA ---


        // --- UPDATE: Calcular 'sector_cuipo', 'producto_ppal', 'cantidad_producto' y 'producto_a_reportar' ---
        const updateQuery = `
            WITH updated_area_data AS (
                SELECT
                    cpf.id,
                    -- Formula 1: sector_cuipo = LEFT([Area Funcional], 4)
                    LEFT(TRIM(cpf.area_funcional), 4) AS new_sector_cuipo,

                    -- Formula 2: producto_ppal = CONCATENAR(SUBSTRING([Area Funcional], 1, 4), SUBSTRING([Area Funcional], 10, 3))
                    CONCAT(
                        SUBSTRING(TRIM(cpf.area_funcional), 1, 4),
                        SUBSTRING(TRIM(cpf.area_funcional), 10, 3)
                    ) AS new_producto_ppal,

                    -- Formula 3: cantidad_producto = CAST(SUBSTRING([Area Funcional], 13, 1) AS INTEGER)
                    CAST(SUBSTRING(TRIM(cpf.area_funcional), 13, 1) AS INTEGER) AS new_cantidad_producto,

                    -- Formula 4: producto_a_reportar = CASE WHEN [Cantidad producto]=1 THEN [Producto Ppal] ELSE 'SELECCIONAR' END
                    CASE
                        WHEN CAST(SUBSTRING(TRIM(cpf.area_funcional), 13, 1) AS INTEGER) = 1
                        THEN CONCAT(SUBSTRING(TRIM(cpf.area_funcional), 1, 4), SUBSTRING(TRIM(cpf.area_funcional), 10, 3))
                        ELSE 'SELECCIONAR'
                    END AS new_producto_a_reportar
                FROM
                    ${process.env.DB_SCHEMA}."cuipo_plantilla_distrito_2025_vf" AS cpf
                WHERE
                    cpf.area_funcional IS NOT NULL AND TRIM(cpf.area_funcional) != '' -- Esta condición AHORA sí debería encontrar datos
            )
            UPDATE ${process.env.DB_SCHEMA}."cuipo_plantilla_distrito_2025_vf" AS dest
            SET
                sector_cuipo = upd.new_sector_cuipo,
                producto_ppal = upd.new_producto_ppal,
                cantidad_producto = upd.new_cantidad_producto,
                producto_a_reportar = upd.new_producto_a_reportar
            FROM updated_area_data AS upd
            WHERE dest.id = upd.id;
        `;

        console.log("Ejecutando UPDATE para 'sector_cuipo', 'producto_ppal', 'cantidad_producto' y 'producto_a_reportar'...");
        const resultUpdates = await client.query(updateQuery);
        console.log(`UPDATE de Parte 5 completado. Registros actualizados: ${resultUpdates.rowCount}`);

        await client.query('COMMIT'); 

        console.log(`Parte 5 completada. Total registros afectados: ${resultUpdates.rowCount}`);

        return res.status(200).json({
            success: true,
            message: "Parte 5 procesada exitosamente: Campos de área funcional y productos actualizados.",
            registros_afectados: resultUpdates.rowCount,
            detalles: {
                acciones_realizadas: [
                    "Copia inicial del campo 'area_funcional' a la plantilla.",
                    "Cálculo del campo 'sector_cuipo' (primeros 4 dígitos de 'area_funcional').",
                    "Cálculo del campo 'producto_ppal' (concatenación de subcadenas de 'area_funcional').",
                    "Cálculo del campo 'cantidad_producto' (dígito en la posición 13 de 'area_funcional').",
                    "Cálculo del campo 'producto_a_reportar' (condicional basado en 'cantidad_producto' y 'producto_ppal')."
                ]
            }
        });

    } catch (error) {
        await client.query('ROLLBACK');
        console.error('❌ Error en procesarParte5:', error.message);

        let userMessage = "Error desconocido al procesar Parte 5.";
        if (error.message.includes("no existe en la base de datos o en el esquema")) {
            userMessage = `La tabla "cuipo_plantilla_distrito_2025_vf" no existe en la base de datos o en el esquema.`;
        } else if (error.message.includes("column") && error.message.includes("does not exist")) {
            userMessage = `Error de columna. Posiblemente un nombre de columna incorrecto en la base de datos: ${error.message}`;
        } else if (error.message.includes("invalid input syntax for type integer")) {
            userMessage = `Error de formato al convertir a número. Asegúrese de que el carácter en la posición 13 de 'area_funcional' sea un dígito.`;
        }
        else {
            userMessage = error.message;
        }

        return res.status(500).json({
            success: false,
            error: "Error al procesar Parte 5",
            detalles: userMessage,
            solucion_sugerida: [
                `Verifique que la tabla ${tablasAValidar[0].schema}.${tablasAValidar[0].name} exista.`, // tablaAValidar[0] es cuipo_plantilla_distrito_2025_vf
                "Asegúrese de que los nombres de las columnas ('area_funcional', 'sector_cuipo', 'producto_ppal', 'cantidad_producto', 'producto_a_reportar') sean correctos.",
                "Confirme que los datos en 'area_funcional' sigan el formato esperado para las extracciones de subcadenas."
            ]
        });
    } finally {
        if (client) {
            client.release(); 
        }
    }
}

// CONTROLADOR PARA LA PARTE 6 - SECTOR SALUD Y EDUCACION
async function procesarParte6(req, res) {
    const client = await pool.connect(); // Obtener una conexión del pool

    try {
        await client.query('BEGIN'); // Iniciar una transacción

        console.log("Iniciando procesamiento de Parte 6: Detalle Sectorial y Programación del Gasto...");

        // 1. Validar existencia de todas las tablas necesarias
        const tablasAValidar = [
            { schema: process.env.DB_SCHEMA, name: 'cuipo_plantilla_distrito_2025_vf' },
            { schema: process.env.DB_SCHEMA, name: 'detalle_sectorial_educacion' },
            { schema: process.env.DB_SCHEMA, name: 'detalle_sectorial_salud' },
            { schema: process.env.DB_SCHEMA, name: 'sectorial_gastos_salud' }
        ];

        for (const tablaInfo of tablasAValidar) {
            const check = await client.query(
                `SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = $1 AND table_schema = $2)`,
                [tablaInfo.name, tablaInfo.schema]
            );
            if (!check.rows[0].exists) {
                throw new Error(`Tabla "${tablaInfo.schema}.${tablaInfo.name}" no existe en la base de datos o en el esquema.`);
            }
        }

        // --- UPDATE Principal para Parte 6 ---
        // Calcula 'detalle_sectorial', 'extrae_detalle_sectorial' y 'detalle_sectorial_prog_gasto'
        const updateQuery = `
            WITH source_data AS (
                SELECT
                    cp.id,
                    cp.secretaria,
                    -- Paso 1: Calcular el 'detalle_sectorial' base que se asignará a la plantilla
                    CASE
                        WHEN TRIM(cp.secretaria) = 'SECRETARÍA DE EDUCACIÓN' THEN
                            (SELECT TRIM(ed.detalle_sectorial) FROM ${process.env.DB_SCHEMA}.detalle_sectorial_educacion AS ed WHERE TRIM(ed.sector) = 'EDUCACION' LIMIT 1)
                        WHEN TRIM(cp.secretaria) = 'SECRETARÍA DE SALUD' THEN
                            (SELECT TRIM(sd.detalle_sectorial) FROM ${process.env.DB_SCHEMA}.detalle_sectorial_salud AS sd WHERE TRIM(sd.sector) = 'SALUD' LIMIT 1)
                        ELSE NULL
                    END AS new_detalle_sectorial_assigned
                FROM
                    ${process.env.DB_SCHEMA}."cuipo_plantilla_distrito_2025_vf" AS cp
            ),
            -- Ahora, sobre los datos ya con el detalle_sectorial asignado, calculamos extrae_detalle_sectorial y luego detalle_sectorial_prog_gasto
            calculated_final_data AS (
                SELECT
                    sd.id,
                    sd.new_detalle_sectorial_assigned,
                    -- Paso 2: Calcular 'extrae_detalle_sectorial'
                    -- Extraer los primeros 8 o 9 caracteres antes del primer ' - '
                    COALESCE(
                        (
                            SELECT
                                CASE
                                    -- Intenta con 8 caracteres primero (ej. 22.01.01)
                                    WHEN
                                        LENGTH(SUBSTRING(TRIM(sd.new_detalle_sectorial_assigned) FROM 1 FOR POSITION(' - ' IN TRIM(sd.new_detalle_sectorial_assigned)) - 1)) = 8
                                        AND EXISTS (SELECT 1 FROM ${process.env.DB_SCHEMA}.detalle_sectorial_educacion WHERE TRIM(codigo) = SUBSTRING(TRIM(sd.new_detalle_sectorial_assigned) FROM 1 FOR 8))
                                        THEN SUBSTRING(TRIM(sd.new_detalle_sectorial_assigned) FROM 1 FOR 8)
                                    WHEN
                                        LENGTH(SUBSTRING(TRIM(sd.new_detalle_sectorial_assigned) FROM 1 FOR POSITION(' - ' IN TRIM(sd.new_detalle_sectorial_assigned)) - 1)) = 8
                                        AND EXISTS (SELECT 1 FROM ${process.env.DB_SCHEMA}.detalle_sectorial_salud WHERE TRIM(codigo) = SUBSTRING(TRIM(sd.new_detalle_sectorial_assigned) FROM 1 FOR 8))
                                        THEN SUBSTRING(TRIM(sd.new_detalle_sectorial_assigned) FROM 1 FOR 8)
                                    -- Si no es 8, intenta con 9 (ej. 19.02.100)
                                    WHEN
                                        LENGTH(SUBSTRING(TRIM(sd.new_detalle_sectorial_assigned) FROM 1 FOR POSITION(' - ' IN TRIM(sd.new_detalle_sectorial_assigned)) - 1)) = 9
                                        AND EXISTS (SELECT 1 FROM ${process.env.DB_SCHEMA}.detalle_sectorial_salud WHERE TRIM(codigo) = SUBSTRING(TRIM(sd.new_detalle_sectorial_assigned) FROM 1 FOR 9))
                                        THEN SUBSTRING(TRIM(sd.new_detalle_sectorial_assigned) FROM 1 FOR 9)
                                    ELSE '0' -- Si no cumple ninguna condición, o el formato es incorrecto, es '0'
                                END
                        ),
                        '0' -- Si new_detalle_sectorial_assigned es NULL o vacío, también es '0'
                    ) AS new_extrae_detalle_sectorial,
                    sd.secretaria
                FROM
                    source_data AS sd
            )
            UPDATE ${process.env.DB_SCHEMA}."cuipo_plantilla_distrito_2025_vf" AS dest
            SET
                detalle_sectorial = cfd.new_detalle_sectorial_assigned,
                extrae_detalle_sectorial = cfd.new_extrae_detalle_sectorial,
                -- Paso 3: Calcular 'detalle_sectorial_prog_gasto' (solo aplica para Salud)
                detalle_sectorial_prog_gasto = COALESCE(
                    (SELECT TRIM(sgs.codigo_programacion_del_gasto)
                     FROM ${process.env.DB_SCHEMA}.sectorial_gastos_salud AS sgs
                     WHERE TRIM(cfd.new_extrae_detalle_sectorial) = TRIM(sgs.codigo_ejecucion_del_gasto)
                       AND TRIM(cfd.secretaria) = 'SECRETARÍA DE SALUD' -- Solo aplicar si es de salud
                     LIMIT 1),
                    '' -- Si no encuentra coincidencia o no es de salud, dejar vacío
                )
            FROM calculated_final_data AS cfd
            WHERE dest.id = cfd.id;
        `;

        console.log("Ejecutando UPDATE para 'detalle_sectorial', 'extrae_detalle_sectorial' y 'detalle_sectorial_prog_gasto'...");
        const resultUpdates = await client.query(updateQuery);
        console.log(`UPDATE de Parte 6 completado. Registros actualizados: ${resultUpdates.rowCount}`);

        await client.query('COMMIT'); // Confirmar la transacción

        console.log(`Parte 6 completada. Total registros afectados: ${resultUpdates.rowCount}`);

        return res.status(200).json({
            success: true,
            message: "Parte 6 procesada exitosamente: Campos de detalle_sectorial, extrae_detalle_sectorial y detalle_sectorial_prog_gasto actualizados.",
            registros_afectados: resultUpdates.rowCount,
            detalles: {
                acciones_realizadas: [
                    "Cálculo y llenado de 'detalle_sectorial' basado en 'secretaria' y tablas 'detalle_sectorial_educacion'/'detalle_sectorial_salud' (campo 'sector').",
                    "Cálculo y llenado de 'extrae_detalle_sectorial' extrayendo el prefijo numérico de 'detalle_sectorial' (8 o 9 caracteres) y validándolo contra los códigos de las tablas de detalle.",
                    "Cálculo y llenado de 'detalle_sectorial_prog_gasto' basado en 'extrae_detalle_sectorial' y la tabla 'sectorial_gastos_salud' (solo para 'SECRETARÍA DE SALUD')."
                ]
            }
        });

    } catch (error) {
        await client.query('ROLLBACK'); // Revertir la transacción en caso de error
        console.error('❌ Error en procesarParte6:', error.message);

        let userMessage = "Error desconocido al procesar Parte 6.";
        if (error.message.includes("no existe en la base de datos o en el esquema")) {
            userMessage = `Una de las tablas necesarias no fue encontrada: ${error.message}`;
        } else if (error.message.includes("column") && error.message.includes("does not exist")) {
            userMessage = `Error de columna. Posiblemente un nombre de columna incorrecto en la base de datos: ${error.message}`;
        } else if (error.message.includes("invalid input syntax for type integer")) {
            userMessage = `Error de formato de datos. Asegúrese de que los datos sean compatibles con las operaciones realizadas.`;
        } else {
            userMessage = error.message;
        }

        return res.status(500).json({
            success: false,
            error: "Error al procesar Parte 6",
            detalles: userMessage,
            solucion_sugerida: [
                `Verifique que las tablas ${tablasAValidar.map(t => `${t.schema}.${t.name}`).join(', ')} existan.`,
                "Asegúrese de que los nombres de las columnas en todas las tablas ('secretaria', 'detalle_sectorial', 'codigo', 'sector', 'codigo_ejecucion_del_gasto', 'codigo_programacion_del_gasto') sean correctos y que los tipos de datos sean compatibles.",
                "Confirme que los valores en 'secretaria' de la plantilla coinciden *exactamente* con 'SECRETARÍA DE EDUCACIÓN' o 'SECRETARÍA DE SALUD'.",
                "Confirme que los valores en 'sector' de las tablas de detalle sean *exactamente* 'EDUCACION' o 'SALUD' respectivamente.",
                "Verifique el formato de los datos en 'detalle_sectorial' de las tablas de detalle. Deben iniciar con un código numérico seguido de ' - ' (ej. '19.01.17 - ABC', '19.02.100 - DEF').",
                "Asegúrese de que los códigos en `detalle_sectorial_educacion.codigo` y `detalle_sectorial_salud.codigo` sean los mismos que la parte numérica al inicio de `detalle_sectorial` en esas mismas tablas (ej. si detalle_sectorial es '22.01.01 - ...', codigo es '22.01.01')."
            ]
        });
    } finally {
        if (client) {
            client.release(); // Liberar la conexión de la pool
        }
    }
}

// CONTROLADOR PARA ENVIAR LOS DATOS DE PRESUPUESTO DE LA TABLA BASE A LA PANTILLA
async function copiarDatosPresupuestales(req, res) {
  const client = await pool.connect(); // Obtener una conexión del pool

  // Lista de campos a copiar. ¡Ahora incluimos 'fondo'!
  const camposACopiar = [
    'fondo', // ¡CRUCIAL! Asegurarse de que 'fondo' se copia
    'ppto_inicial', 'reducciones', 'adiciones',
    'creditos', 'contracreditos', 'total_ppto_actual',
    'disponibilidad', 'compromiso', 'factura',
    'pagos', 'disponible_neto', 'ejecucion', '_ejecucion'
  ];

  try {
    await client.query('BEGIN'); // Iniciar una transacción

    const tablaOrigen = 'base_de_ejecucion_presupuestal_30062025';
    const tablaDestino = 'cuipo_plantilla_distrito_2025_vf';
    const schema = process.env.DB_SCHEMA;

    // 1. Validar existencia de las tablas (esto ya lo tienes y está bien)
    const tablasAValidar = [
      { schema: schema, name: tablaOrigen },
      { schema: schema, name: tablaDestino }
    ];

    for (const tablaInfo of tablasAValidar) {
      const check = await client.query(
        `SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = $1 AND table_schema = $2)`,
        [tablaInfo.name, tablaInfo.schema]
      );
      if (!check.rows[0].exists) {
        throw new Error(`Tabla "${tablaInfo.schema}.${tablaInfo.name}" no existe en la base de datos o en el esquema.`);
      }
    }

    // --- PASO CRÍTICO: Vaciar la tabla de destino y reiniciar su secuencia de ID ---
    // Usamos TRUNCATE ... RESTART IDENTITY; porque 'id' es SERIAL
    await client.query(`TRUNCATE TABLE ${schema}."${tablaDestino}" RESTART IDENTITY;`);
    console.log(`Tabla ${tablaDestino} vaciada y secuencia de ID reiniciada.`);
    // --- FIN DEL PASO CRÍTICO ---

    // 2. Construir la consulta de inserción dinámica. Ya no usamos UPSERT.
    
    // Nombres de columnas para la inserción
    const columnasDestino = ['id', ...camposACopiar].map(col => `"${col}"`).join(', ');
    
    // Valores de las columnas de origen
    const columnasOrigen = ['id', ...camposACopiar].map(col => `orig."${col}"`).join(', ');

    const insertQuery = `
      INSERT INTO ${schema}."${tablaDestino}" (${columnasDestino})
      SELECT ${columnasOrigen}
      FROM ${schema}."${tablaOrigen}" AS orig
      ORDER BY orig.id; -- Aseguramos el orden de inserción por ID
    `;

    console.log("Consulta SQL de INSERCIÓN:", insertQuery); // Para depuración

    const result = await client.query(insertQuery);
    await client.query('COMMIT'); // Confirmar la transacción

    console.log(`Copia de datos presupuestales completada. Registros insertados: ${result.rowCount}`);

    return res.status(200).json({
      success: true,
      message: `Datos presupuestales copiados de "${tablaOrigen}" a "${tablaDestino}" exitosamente. Se insertaron ${result.rowCount} registros.`,
      registros_copiados: result.rowCount,
      campos_copiados: camposACopiar,
      detalles: "La tabla de destino fue vaciada y luego se insertaron los nuevos datos."
    });

  } catch (error) {
    await client.query('ROLLBACK'); // Revertir la transacción en caso de error
    console.error('❌ Error en copiarDatosPresupuestales:', error.message);

    let userMessage = "Error desconocido al copiar datos presupuestales.";
    if (error.message.includes("no existe en la base de datos o en el esquema")) {
      userMessage = `Una de las tablas necesarias no fue encontrada: ${error.message}`;
    } else if (error.message.includes("column") && error.message.includes("does not exist")) {
      userMessage = `Error de columna. Posiblemente un nombre de columna incorrecto en la base de datos: ${error.message}. Asegúrate de que todas las columnas en la lista ${camposACopiar.join(', ')} existan en ambas tablas y que haya una columna 'id' como clave primaria.`;
    } else if (error.message.includes("duplicate key value violates unique constraint")) {
      userMessage = `Error de clave duplicada. Asegúrate de que la columna 'id' en "${tablaDestino}" es una clave primaria o única y que la tabla fue vaciada correctamente si es la intención de sobrescribir.`;
    }
    else {
      userMessage = error.message;
    }

    return res.status(500).json({
      success: false,
      error: "Error al copiar datos presupuestales",
      detalles: userMessage,
      solucion_sugerida: [
        `Verifique que las tablas "${tablaOrigen}" y "${tablaDestino}" existan en el esquema ${process.env.DB_SCHEMA}.`,
        "Confirme que todas las columnas en la lista de campos a copiar existen en AMBAS tablas.",
        `Asegúrese de que la tabla "${tablaDestino}" puede recibir los IDs de la tabla de origen sin conflictos de secuencia si 'id' es SERIAL/IDENTITY.`,
        "Verifique los permisos de usuario de la base de datos para TRUNCATE/DELETE e INSERT."
      ]
    });
  } finally {
    client.release(); // Liberar la conexión al pool
  }
}

// CONTROLADOR PARA OBTENER LAS OPCIONES DEL CPC
async function getCpcOptions(req, res) {
    const { lastDigit } = req.params; // Obtenemos el último dígito del parámetro de la URL
    let client;

    if (!lastDigit || !/^\d$/.test(lastDigit)) { // Validar que sea un solo dígito
        return res.status(400).json({
            success: false,
            message: "Parámetro 'lastDigit' inválido. Debe ser un solo dígito (0-9)."
        });
    }

    try {
        client = await pool.connect();

        // Validar existencia de la tabla 'cpc'
        const check = await client.query(
            `SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = $1 AND table_schema = $2)`,
            ['cpc', process.env.DB_SCHEMA]
        );
        if (!check.rows[0].exists) {
            throw new Error(`Tabla "${process.env.DB_SCHEMA}.cpc" no existe en la base de datos o en el esquema.`);
        }

        const query = `
            SELECT
                TRIM(codigo_clase_o_subclase) AS option_label,
                TRIM(codigo_clase_o_subclase) AS option_value -- Usamos el mismo para valor y etiqueta, puedes cambiar 'codigo' si lo prefieres
            FROM
                ${process.env.DB_SCHEMA}.cpc
            WHERE
                TRIM(cpc) = $1
            ORDER BY
                codigo_clase_o_subclase;
        `;

        const result = await client.query(query, [lastDigit]);

        // Formatear el resultado para el frontend ( [{ label: '...', value: '...' }] )
        const options = result.rows.map(row => ({
            label: row.option_label,
            value: row.option_value
        }));

        res.status(200).json({
            success: true,
            data: options
        });

    } catch (error) {
        console.error('❌ Error al obtener opciones de CPC:', error.message);
        let userMessage = "Error interno del servidor al obtener opciones de CPC.";
        if (error.message.includes("no existe en la base de datos o en el esquema")) {
            userMessage = `La tabla "cpc" no fue encontrada: ${error.message}`;
        }
        res.status(500).json({
            success: false,
            message: userMessage,
            solucion_sugerida: "Verifique la existencia y el nombre de la tabla 'cpc' y sus columnas en la base de datos."
        });
    } finally {
        if (client) {
            client.release();
        }
    }
}

async function actualizarFila(req, res) {
    // Desestructurar todos los campos relevantes que pueden llegar en el body
    const { 
        id, 
        codigo_y_nombre_del_cpc, 
        cpc_cuipo, 
        validador_cpc,
        codigo_y_nombre_del_producto_mga, // ¡NUEVO!
        producto_cuipo,                   // ¡NUEVO!
        validador_del_producto            // ¡NUEVO!
    } = req.body;

    let client;

    if (!id) {
        return res.status(400).json({ success: false, message: "ID de registro es requerido para la actualización." });
    }

    try {
        client = await pool.connect();
        await client.query('BEGIN');

        // Construir la consulta de UPDATE dinámicamente
        const updates = [];
        const values = [];
        let paramIndex = 1;

        // Lógica para campos CPC
        if (codigo_y_nombre_del_cpc !== undefined) {
            updates.push(`codigo_y_nombre_del_cpc = $${paramIndex++}`);
            values.push(codigo_y_nombre_del_cpc);
        }
        if (cpc_cuipo !== undefined) {
            updates.push(`cpc_cuipo = $${paramIndex++}`);
            values.push(cpc_cuipo);
        }
        if (validador_cpc !== undefined) {
            updates.push(`validador_cpc = $${paramIndex++}`);
            values.push(validador_cpc);
        }

        // --- ¡NUEVA LÓGICA para campos de Producto MGA! ---
        if (codigo_y_nombre_del_producto_mga !== undefined) {
            updates.push(`codigo_y_nombre_del_producto_mga = $${paramIndex++}`);
            values.push(codigo_y_nombre_del_producto_mga);
        }
        if (producto_cuipo !== undefined) {
            updates.push(`producto_cuipo = $${paramIndex++}`);
            values.push(producto_cuipo);
        }
        if (validador_del_producto !== undefined) {
            updates.push(`validador_del_producto = $${paramIndex++}`);
            values.push(validador_del_producto);
        }
        // ----------------------------------------------------

        if (updates.length === 0) {
            await client.query('ROLLBACK');
            return res.status(400).json({ success: false, message: "No se proporcionaron campos para actualizar." });
        }

        values.push(id); // Añadir el ID al final de los valores

        const updateQuery = `
            UPDATE ${process.env.DB_SCHEMA}."cuipo_plantilla_distrito_2025_vf"
            SET ${updates.join(', ')}
            WHERE id = $${paramIndex}
        `;

        const result = await client.query(updateQuery, values);

        if (result.rowCount === 0) {
            await client.query('ROLLBACK');
            return res.status(404).json({ success: false, message: "Registro no encontrado para actualizar." });
        }

        await client.query('COMMIT');
        res.status(200).json({ success: true, message: "Fila actualizada exitosamente." });

    } catch (error) {
        await client.query('ROLLBACK');
        console.error('❌ Error al actualizar fila:', error.message);
        res.status(500).json({
            success: false,
            message: "Error interno del servidor al actualizar la fila.",
            error_details: error.message
        });
    } finally {
        if (client) {
            client.release();
        }
    }
}

// CONTROLADOR DE PRODUCTO MGA
async function getProductosMgaOptions(req, res) {
  try {
    const { codigoSap } = req.query;

    if (!codigoSap || codigoSap.trim() === '') {
      return res.status(400).json({
        success: false,
        message: 'El código SAP/proyecto es requerido'
      });
    }

    // 1. Buscar TODOS los productos asociados al código SAP
    const query = `
      SELECT 
        productos_del_proyecto,
        cod_pdto_y_nombre
      FROM 
        sis_catastro_verificacion.productos_por_proyecto
      WHERE 
        codigo_sap = $1
      ORDER BY 
        cod_pdto_y_nombre ASC;
    `;

    const { rows: productos } = await pool.query(query, [codigoSap]);

    // 2. Determinar la cantidad de productos
    const cantidad_producto = productos.length;

    // 3. Formatear opciones para el select
    const options = productos.map(producto => ({
      value: producto.cod_pdto_y_nombre,  // Esto va al select
      label: producto.cod_pdto_y_nombre,  // Esto se muestra en la UI
      producto_codigo: producto.productos_del_proyecto  // Esto va a producto_cuipo
    }));

    res.json({
      success: true,
      data: {
        options,
        cantidad_producto,
        message: cantidad_producto > 0 
          ? `${cantidad_producto} productos encontrados` 
          : 'No hay productos para este proyecto'
      }
    });

  } catch (error) {
    console.error('Error en getProductosMgaOptions:', error);
    res.status(500).json({
      success: false,
      message: 'Error al cargar productos',
      error: error.message
    });
  }
};

// NUEVO PARA VALIDAR EL PRODUCT MGA
async function validarProductoController(req, res) {
    try {
        const { codigo_y_nombre_del_producto_mga } = req.body;

        console.log('✅ Validando producto:', codigo_y_nombre_del_producto_mga);

        // 1. Validar que venga el código
        if (!codigo_y_nombre_del_producto_mga) {
            return res.status(400).json({
                success: false,
                message: 'El código/nombre del producto MGA es requerido'
            });
        }

        // 2. ✅ USAR LA MISMA TABLA QUE YA FUNCIONA: productos_por_proyecto
        const query = `
            SELECT 
                productos_del_proyecto,
                cod_pdto_y_nombre
            FROM 
                sis_catastro_verificacion.productos_por_proyecto
            WHERE 
                cod_pdto_y_nombre = $1
            LIMIT 1;
        `;

        const result = await pool.query(query, [codigo_y_nombre_del_producto_mga]);

        // 3. Lógica de validación SIMPLE
        let validador_del_producto = 'PENDIENTE';
        let es_valido = false;

        if (result.rows.length > 0) {
            // ✅ Producto existe en la tabla que ya usamos
            validador_del_producto = 'PRODUCTO OK';
            es_valido = true;
        } else {
            // ❌ Producto no encontrado
            validador_del_producto = 'NO VALIDADO';
            es_valido = false;
        }

        res.json({
            success: true,
            data: {
                validador_del_producto,
                es_valido,
                producto_validado: codigo_y_nombre_del_producto_mga,
                producto_codigo: result.rows[0]?.productos_del_proyecto || null
            }
        });

    } catch (error) {
        console.error('Error validando producto:', error);
        res.status(500).json({
            success: false,
            message: 'Error interno al validar producto',
            error: error.message
        });
    }
}

// CONTROLADOR PARA OBTENER LA CANTIDAD DE PROYECTOS POS SECRETARIA
async function getProyectosPorSecretariaController(req, res) {
    try {
        // Validación de autenticación y roles
        if (!req.user || !req.user.id_role_user) {
            console.log('ERROR - Usuario no autenticado o rol no definido:', {
                user: req.user,
                id_role_user: req.user?.id_role_user
            });
            return res.status(401).json({
                success: false,
                message: "Usuario no autenticado o rol no definido"
            });
        }

        // Determinar si el usuario es admin o juanlaver
        const isAdmin = [1, 2].includes(req.user.id_role_user);
        const isJuanLaver = req.user.id_role_user === 1;

        // Solo aplicar filtro de dependencia si no es admin ni juanlaver
        const dependenciaUsuario = (!isAdmin && !isJuanLaver) ? req.user.dependencyName : null;

        console.log('DEBUG - Procesando solicitud de proyectos por secretaría:', {
            userId: req.user.id,
            role: req.user.role,
            id_role_user: req.user.id_role_user,
            isAdmin,
            isJuanLaver,
            dependencyName: req.user.dependencyName,
            appliedDependencia: dependenciaUsuario
        });

        const { success, data, message: modelMessage, error: modelError } = 
            await estadisticasModel.getProyectosPorSecretaria(dependenciaUsuario);

        if (!success) {
            console.log('ERROR - Fallo al obtener proyectos por secretaría:', {
                message: modelMessage,
                error: modelError,
                dependenciaUsuario
            });
            return res.status(500).json({
                success: false,
                message: modelMessage || "Error al procesar la consulta de proyectos",
                error: modelError
            });
        }

        console.log('INFO - Proyectos por secretaría obtenidos exitosamente:', {
            totalRegistros: data?.length || 0,
            filtroDependencia: dependenciaUsuario ? 'Aplicado' : 'No aplicado'
        });

        return res.status(200).json({
            success: true,
            data,
            filteredByDependency: !!dependenciaUsuario
        });
    } catch (error) {
        console.error("Error en el controlador getProyectosPorSecretariaController:", error);
        return res.status(500).json({ success: false, message: "Error interno del servidor al obtener proyectos por secretaría." });
    }
}

// CONTROLADOR PARA OBTENER EL DETALLE DE CADA PROYECTO POR SECRETARIA
async function getDetalleProyectoController(req, res) {
    try {
        const { secretaria, proyecto } = req.query;
        const dependenciaUsuario = req.user?.dependencyName || null;

        const decodedSecretaria = secretaria ? decodeURIComponent(secretaria) : null;
        const decodedProyecto = proyecto ? decodeURIComponent(proyecto) : null;

        const { success, data, message: modelMessage, error: modelError } = 
            await estadisticasModel.getDetalleProyecto(
                dependenciaUsuario,
                decodedSecretaria,
                decodedProyecto
            );

        if (!success) {
            return res.status(500).json({ success: false, message: modelMessage || "Error del servidor", error: modelError });
        }

        return res.status(200).json({ success: true, data });
    } catch (error) {
        console.error("Error en el controlador getDetalleProyectoController:", error);
        return res.status(500).json({ success: false, message: "Error interno del servidor al obtener detalle del proyecto." });
    }
}

async function getDatosGraficaProyectoController(req, res) {
    try {
        const { secretaria, proyecto } = req.query;
        
        console.log('DEBUG - Controller gráfica - Parámetros recibidos:', {
            secretaria, 
            proyecto
        });

        if (!secretaria || !proyecto) {
            return res.status(400).json({
                success: false,
                message: "Los parámetros 'secretaria' y 'proyecto' son requeridos"
            });
        }

        const { success, data, message, error } = 
            await estadisticasModel.getDatosParaGraficaProyecto(secretaria, proyecto);

        if (!success) {
            return res.status(500).json({
                success: false,
                message: message || "Error al obtener datos para gráfica",
                error
            });
        }

        return res.status(200).json({
            success: true,
            data
        });

    } catch (error) {
        console.error("Error en controlador gráfica proyecto:", error);
        return res.status(500).json({
            success: false,
            message: "Error interno del servidor",
            error: error.message
        });
    }
}

// CONTROLADOR PARA EL CONTEO DE LAS VALIDACIONES
async function getValidationSummary(req, res) {
    let client;
    try {
        client = await pool.connect();

        // Consulta para CPC - ESTAS CONSULTAS ESTÁN CORRECTAS SEGÚN TU VALIDACIÓN
        const cpcQuery = `
            SELECT
                COUNT(*) FILTER (WHERE validador_cpc = 'CPC OK') AS cpc_ok,
                COUNT(*) FILTER (WHERE validador_cpc IS NULL OR validador_cpc = '') AS cpc_faltantes
            FROM ${process.env.DB_SCHEMA}."cuipo_plantilla_distrito_2025_vf";
        `;
        const cpcResult = await client.query(cpcQuery);
        const cpcSummary = cpcResult.rows[0];

        // Consulta para Producto MGA - ESTAS CONSULTAS ESTÁN CORRECTAS SEGÚN TU VALIDACIÓN
        const productQuery = `
            SELECT
                COUNT(*) FILTER (WHERE validador_del_producto = 'PRODUCTO OK') AS producto_ok,
                COUNT(*) FILTER (WHERE validador_del_producto IS NULL OR validador_del_producto = '') AS producto_faltantes
            FROM ${process.env.DB_SCHEMA}."cuipo_plantilla_distrito_2025_vf";
        `;
        const productResult = await client.query(productQuery);
        const productSummary = productResult.rows[0];

        res.json({
            success: true,
            data: {
                cpc: {
                    ok: parseInt(cpcSummary.cpc_ok, 10),
                    faltantes: parseInt(cpcSummary.cpc_faltantes, 10)
                },
                producto: {
                    ok: parseInt(productSummary.producto_ok, 10),
                    faltantes: parseInt(productSummary.producto_faltantes, 10)
                }
            }
        });

    } catch (error) {
        console.error('Error al obtener resumen de validaciones:', error);
        res.status(500).json({
            success: false,
            message: 'Error interno del servidor al obtener resumen de validaciones.',
            error_details: error.message
        });
    } finally {
        if (client) {
            client.release();
        }
    }
}

async function getMissingDetails(req, res) {
    const { tipo_validador } = req.params;
    let client;

    let validadorColumn = '';
    if (tipo_validador === 'cpc') {
        validadorColumn = 'validador_cpc';
    } else if (tipo_validador === 'producto') {
        validadorColumn = 'validador_del_producto';
    } else {
        return res.status(400).json({ success: false, message: "Tipo de validador no válido." });
    }

    try {
        client = await pool.connect();

        const query = `
            SELECT
                secretaria,   -- ¡CAMBIO AQUÍ! Nombre de columna corregido
                nombre_proyecto AS nombre_del_proyecto_o_codigo_sap -- ¡CAMBIO AQUÍ! Nombre de columna corregido
            FROM ${process.env.DB_SCHEMA}."cuipo_plantilla_distrito_2025_vf"
            WHERE ${validadorColumn} IS NULL OR ${validadorColumn} = '';
        `;

        const { rows } = await client.query(query);

        res.json({
            success: true,
            data: rows
        });

    } catch (error) {
        console.error('Error al obtener detalles de faltantes:', error);
        res.status(500).json({
            success: false,
            message: 'Error interno del servidor al obtener detalles de faltantes.',
            error_details: error.message
        });
    } finally {
        if (client) {
            client.release();
        }
    }
}

module.exports = {
  uploadExcel,
  listTables,
  getTableData,
  tablasDisponibles,
  procesarParte1,
  procesarParte2,
  procesarParte3,
  procesarParte4,
  procesarParte5,
  procesarParte6,
  copiarDatosPresupuestales,
  getCpcOptions,
  actualizarFila,
  getProductosMgaOptions,
  validarProductoController,
  getProyectosPorSecretariaController,
  getDetalleProyectoController,
  getDatosGraficaProyectoController,
  getValidationSummary,
  getMissingDetails
}