// Asegúrate de importar el pool de conexiones si no lo has hecho
const { pool } = require("../db");

async function getProyectosPorSecretaria(dependencia = null) {
    try {
        console.log('DEBUG - Model: getProyectosPorSecretaria llamado con dependencia:', dependencia);
        
        let query = `
            WITH proyectos_base AS (
                SELECT
                    TRIM(cp.secretaria) AS secretaria,
                    TRIM(cp.proyecto) AS codigo,
                    TRIM(p.nombre_proyecto) AS nombre,
                    TRIM(cp.fuente) AS fuente,
                    TRIM(d.centro_gestor) AS centro_gestor,
                    TRIM(d.dependencia) AS dependencia_nombre_completo
                FROM
                    ${process.env.DB_SCHEMA}.cuipo_plantilla_distrito_2025_vf AS cp
                LEFT JOIN
                    ${process.env.DB_SCHEMA}.dependencias AS d ON TRIM(cp.secretaria) = TRIM(d.dependencia) AND TRIM(cp.centro_gestor) = TRIM(d.centro_gestor)
                LEFT JOIN
                    ${process.env.DB_SCHEMA}.proyectos AS p ON TRIM(cp.proyecto) = TRIM(p.proyecto)
                WHERE
                    TRIM(cp.secretaria) IS NOT NULL AND TRIM(cp.secretaria) != ''
        `;

        const params = [];
        let paramIndex = 1;

        // Filtrar solo si el usuario tiene dependencia asignada
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
        )
        SELECT 
            pb.secretaria,
            COUNT(*) as total_proyectos,
            pb.centro_gestor,
            pb.dependencia_nombre_completo,
            array_agg(
                jsonb_build_object(
                    'codigo', pb.codigo,
                    'nombre', pb.nombre,
                    'fuente', pb.fuente
                )
                ORDER BY pb.codigo
            ) as proyectos
        FROM 
            proyectos_base pb
        GROUP BY
            pb.secretaria,
            pb.centro_gestor,
            pb.dependencia_nombre_completo
        ORDER BY
            pb.secretaria
        `;

        console.log('DEBUG - Model: Query final:', {
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
        console.error("Error al obtener proyectos por secretaría:", error);
        return { success: false, message: "Error al obtener proyectos por secretaría.", error: error.message };
    }
}

async function getDetalleProyecto(dependencia = null, secretariaNombre = null, proyectoNombre = null) {
    try {
        console.log('🔍 DEBUG - Esquema correcto: usando sis_catastro_verificacion');

        let query = `
            SELECT
                TRIM(fuente) AS fuente,
                TRIM(secretaria) AS secretaria,
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
            FROM
                sis_catastro_verificacion.cuipo_plantilla_distrito_2025_vf  -- ✅ ESQUEMA CORRECTO
            WHERE 1=1
        `;

        const params = [];
        let paramIndex = 1;

        // Siempre filtrar por secretaría si se proporciona
        if (secretariaNombre) {
            query += ` AND TRIM(secretaria) = $${paramIndex}`;
            params.push(secretariaNombre.trim());
            paramIndex++;
        }

        // Filtrar por proyecto EXACTO
        if (proyectoNombre) {
            query += ` AND TRIM(proyecto) = $${paramIndex}`;
            params.push(proyectoNombre.trim());
            paramIndex++;
        }

        console.log('🔍 DEBUG - Consulta con esquema correcto:', {
            query: query.replace(/\s+/g, ' '),
            params,
            secretariaNombre, 
            proyectoNombre
        });

        const result = await pool.query(query, params);
        
        console.log('🔍 DEBUG - Resultados con esquema correcto:', {
            filasEncontradas: result.rows.length,
            parametrosBusqueda: { secretariaNombre, proyectoNombre }
        });

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

async function getDatosParaGraficaProyecto(secretariaNombre = null, proyectoNombre = null) {
    try {
        console.log('DEBUG - Obteniendo datos para gráfica:', { secretariaNombre, proyectoNombre });

        let query = `
            SELECT
                -- Campos para la gráfica
                ppto_inicial,
                total_ppto_actual,
                disponibilidad,
                disponible_neto,
                _ejecucion as ejecucion_porcentaje,
                
                -- Campos de identificación
                TRIM(secretaria) AS secretaria,
                TRIM(proyecto) AS proyecto,
                TRIM(nombre_proyecto) AS nombre_proyecto,
                TRIM(fuente) AS fuente
                
            FROM ${process.env.DB_SCHEMA}.cuipo_plantilla_distrito_2025_vf
            WHERE 1=1
        `;

        const params = [];
        let paramIndex = 1;

        // Filtros obligatorios
        if (secretariaNombre) {
            query += ` AND TRIM(secretaria) = $${paramIndex}`;
            params.push(secretariaNombre.trim());
            paramIndex++;
        }

        if (proyectoNombre) {
            query += ` AND TRIM(proyecto) = $${paramIndex}`;
            params.push(proyectoNombre.trim());
            paramIndex++;
        }

        query += ` LIMIT 1`; // Solo necesitamos un registro

        console.log('DEBUG - Query para gráfica:', {
            query: query.replace(/\s+/g, ' '),
            params
        });

        const result = await pool.query(query, params);
        
        console.log('DEBUG - Datos para gráfica encontrados:', {
            filas: result.rows.length,
            datos: result.rows[0] || null
        });

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
    // ... otras exportaciones ...
    getProyectosPorSecretaria,
    getDetalleProyecto,
    getDatosParaGraficaProyecto
};