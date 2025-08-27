// Asegúrate de importar el pool de conexiones si no lo has hecho
const { pool } = require("../db");

async function getProyectosPorSecretaria(dependencia = null) {
    try {
        console.log('DEBUG - Model: getProyectosPorSecretaria llamado con dependencia:', dependencia);

        let query = `
            WITH proyectos_secretarias AS (
                SELECT
                    TRIM(cp.secretaria) AS nombre,
                    TRIM(cp.proyecto) AS codigo,
                    TRIM(p.nombre_proyecto) AS nombre_proyecto,
                    TRIM(cp.fuente) AS fuente,
                    TRIM(d.centro_gestor) AS centro_gestor,
                    TRIM(d.dependencia) AS dependencia_nombre_completo,
                    'secretaria'::text AS tipo
                FROM
                    ${process.env.DB_SCHEMA}.cuipo_plantilla_distrito_2025_vf AS cp
                LEFT JOIN
                    ${process.env.DB_SCHEMA}.dependencias AS d 
                        ON TRIM(cp.secretaria) = TRIM(d.dependencia) 
                        AND TRIM(cp.centro_gestor) = TRIM(d.centro_gestor)
                LEFT JOIN
                    ${process.env.DB_SCHEMA}.proyectos AS p 
                        ON TRIM(cp.proyecto) = TRIM(p.proyecto)
                WHERE
                    TRIM(cp.secretaria) IS NOT NULL 
                    AND TRIM(cp.secretaria) != ''
        `;

        const params = [];
        let paramIndex = 1;

        // 🔹 Filtro para dependencias (solo si aplica)
        if (dependencia) {
            query += ` AND (
                TRIM(cp.secretaria) = $${paramIndex} OR
                TRIM(d.centro_gestor) = $${paramIndex} OR
                TRIM(d.dependencia) = $${paramIndex}
            )`;
            params.push(dependencia);
            paramIndex++;
        }

        query += `
            ),
            proyectos_estapublicos AS (
                SELECT
                    TRIM(e.establecimiento_publico) AS nombre,
                    TRIM(e.proyecto) AS codigo,
                    TRIM(e.nombre) AS nombre_proyecto,
                    NULL::text AS fuente, -- la tabla estapublicos no tiene fuente, así que lo dejamos nulo
                    TRIM(e.centro_gestor) AS centro_gestor,
                    TRIM(e.establecimiento_publico) AS dependencia_nombre_completo,
                    'establecimiento'::text AS tipo
                FROM
                    ${process.env.DB_SCHEMA}.estapublicos AS e
                WHERE
                    TRIM(e.establecimiento_publico) IS NOT NULL 
                    AND TRIM(e.establecimiento_publico) != ''
        `;

        // 🔹 Filtro para dependencias también en estapublicos
        if (dependencia) {
            query += ` AND (
                TRIM(e.centro_gestor) = $${paramIndex} OR
                TRIM(e.establecimiento_publico) = $${paramIndex}
            )`;
            params.push(dependencia);
            paramIndex++;
        }

        query += `
            )
            SELECT 
                pb.nombre AS secretaria,
                COUNT(DISTINCT pb.codigo) as total_proyectos,  -- ✅ Proyectos únicos
                pb.centro_gestor,
                pb.dependencia_nombre_completo,
                pb.tipo,
                array_agg(
                    jsonb_build_object(
                        'codigo', pb.codigo,
                        'nombre', pb.nombre_proyecto,
                        'fuente', pb.fuente
                    )
                    ORDER BY pb.codigo
                ) as proyectos
            FROM (
                SELECT * FROM proyectos_secretarias
                UNION ALL
                SELECT * FROM proyectos_estapublicos
            ) pb
            GROUP BY
                pb.nombre,
                pb.centro_gestor,
                pb.dependencia_nombre_completo,
                pb.tipo
            ORDER BY
                pb.nombre;
        `;

        console.log('DEBUG - Model: Query final con secretarías + estapublicos:', {
            query: query.replace(/\s+/g, ' '),
            params
        });

        const result = await pool.query(query, params);
        console.log('DEBUG - Model: Número de filas retornadas:', result.rows.length);

        return { 
            success: true, 
            data: result.rows.map(row => ({
                ...row,
                total_proyectos: parseInt(row.total_proyectos, 10)
            }))
        };
    } catch (error) {
        console.error("Error al obtener proyectos por secretaría o establecimiento:", error);
        return { success: false, message: "Error al obtener proyectos por secretaría o establecimiento.", error: error.message };
    }
}

async function getDetalleProyecto(dependencia = null, secretariaNombre = null, proyectoCodigoONombre = null) {
    try {
        console.log('🔍 DEBUG - Obteniendo detalle de proyecto:', { secretariaNombre, proyectoCodigoONombre });

        const params = [];
        let paramIndex = 1;

        const query = `
            SELECT DISTINCT ON (TRIM(proyecto))
                TRIM(fuente) AS fuente,
                TRIM(secretaria) AS dependencia,
                TRIM(pospre) AS pospre,
                TRIM(proyecto) AS proyecto_,
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
            FROM sis_catastro_verificacion.cuipo_plantilla_distrito_2025_vf
            WHERE TRIM(secretaria) = $${paramIndex++}
            AND TRIM(proyecto) = $${paramIndex}
            ORDER BY TRIM(proyecto), TRIM(fuente)
        `;
        params.push(secretariaNombre.trim(), proyectoCodigoONombre.trim());

        console.log("🔍 DEBUG - Query detalle proyecto:", {
            query: query.replace(/\s+/g, ' '),
            params
        });

        const result = await pool.query(query, params);

        return {
            success: true,
            data: result.rows,
            total: result.rows.length
        };

    } catch (error) {
        console.error("Error en getDetalleProyecto:", error);
        return {
            success: false,
            message: "Error al obtener detalle del proyecto.",
            error: error.message
        };
    }
}

// 🔹 Datos resumidos para gráfica de un proyecto
async function getDatosParaGraficaProyecto(secretariaNombre = null, proyectoCodigoONombre = null) {
    try {
        console.log('DEBUG - Obteniendo datos para gráfica:', { secretariaNombre, proyectoCodigoONombre });

        const params = [];
        let paramIndex = 1;

        const query = `
            SELECT DISTINCT ON (TRIM(proyecto))
                ppto_inicial,
                total_ppto_actual,
                disponibilidad,
                disponible_neto,
                _ejecucion as ejecucion_porcentaje,
                TRIM(secretaria) AS dependencia,
                TRIM(proyecto) AS proyecto,
                TRIM(nombre_proyecto) AS nombre_proyecto,
                TRIM(fuente) AS fuente
            FROM sis_catastro_verificacion.cuipo_plantilla_distrito_2025_vf
            WHERE TRIM(secretaria) = $${paramIndex++}
            AND TRIM(proyecto) = $${paramIndex}
            ORDER BY TRIM(proyecto), TRIM(fuente)
        `;
        params.push(secretariaNombre.trim(), proyectoCodigoONombre.trim());

        console.log("DEBUG - Query para gráfica:", {
            query: query.replace(/\s+/g, ' '),
            params
        });

        const result = await pool.query(query, params);

        console.log("⚡DEBUG - Resultados detalle:", result.rows[0]);
        return { 
            success: true, 
            data: result.rows.length > 0 ? result.rows[0] : null
        };

    } catch (error) {
        console.error("Error al obtener datos para gráfica:", error);
        return { 
            success: false, 
            message: "Error al obtener datos para gráfica.", 
            error: error.message 
        };
    }
}

module.exports = {
    getProyectosPorSecretaria,
    getDetalleProyecto,
    getDatosParaGraficaProyecto
};