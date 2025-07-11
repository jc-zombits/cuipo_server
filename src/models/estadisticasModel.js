// Asegúrate de importar el pool de conexiones si no lo has hecho
const { pool } = require("../db"); // Asume que tienes esta ruta

// ... otras funciones ...

async function getProyectosPorSecretaria() {
    try {
        const query = `
            SELECT
                TRIM(cp.secretaria) AS secretaria,
                COUNT(DISTINCT TRIM(cp.proyecto)) AS total_proyectos,
                TRIM(d.centro_gestor) AS centro_gestor,
                TRIM(d.dependencia) AS dependencia_nombre_completo -- Opcional, si quieres tener el nombre original de la dependencia
            FROM
                ${process.env.DB_SCHEMA}.cuipo_plantilla_distrito_2025_vf AS cp
            LEFT JOIN
                ${process.env.DB_SCHEMA}.dependencias AS d ON TRIM(cp.secretaria) = TRIM(d.dependencia)
            WHERE
                TRIM(cp.secretaria) IS NOT NULL AND TRIM(cp.secretaria) != ''
            GROUP BY
                TRIM(cp.secretaria),
                TRIM(d.centro_gestor),
                TRIM(d.dependencia) -- Si incluyes dependencia_nombre_completo, también debe ir aquí
            ORDER BY
                TRIM(cp.secretaria);
        `;
        const result = await pool.query(query);
        return { success: true, data: result.rows };
    } catch (error) {
        console.error("Error al obtener proyectos por secretaría:", error);
        return { success: false, message: "Error al obtener proyectos por secretaría.", error: error.message };
    }
}

async function getDetalleProyecto(secretariaNombre = null, proyectoNombre = null) {
    try {
        let query = `
            SELECT
                TRIM(fuente) AS fuente,
                TRIM(secretaria) AS secretaria,
                TRIM(pospre) AS pospre,
                TRIM(proyecto) AS proyecto_, -- Renombrado para evitar conflicto con palabra reservada 'proyecto'
                TRIM(nombre_proyecto) AS nombre_proyecto,
                ppto_inicial,
                reducciones,
                adiciones,
                creditos,
                contracreditos,
                total_ppto_actual,
                disponibilidad,
                compromiso,
                factura,
                pagos,
                disponible_neto,
                ejecucion,
                _ejecucion
            FROM
                ${process.env.DB_SCHEMA}.cuipo_plantilla_distrito_2025_vf
            WHERE 1=1
        `;
        const params = [];
        let paramIndex = 1;

        if (secretariaNombre) {
            query += ` AND TRIM(secretaria) = $${paramIndex}`;
            params.push(secretariaNombre);
            paramIndex++;
        }
        if (proyectoNombre) {
            query += ` AND TRIM(proyecto) = $${paramIndex}`;
            params.push(proyectoNombre);
            paramIndex++;
        }
        
        // Agregamos un ORDER BY para consistencia, por ejemplo, por secretaria y luego por proyecto
        query += ` ORDER BY TRIM(secretaria), TRIM(proyecto)`;

        const result = await pool.query(query, params);
        return { success: true, data: result.rows };
    } catch (error) {
        console.error("Error al obtener detalle del proyecto (model):", error);
        return { success: false, message: "Error al obtener detalle del proyecto.", error: error.message };
    }
}

module.exports = {
    // ... otras exportaciones ...
    getProyectosPorSecretaria,
    getDetalleProyecto
};