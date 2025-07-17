const jwt = require('jsonwebtoken');
const { pool } = require('../db/index'); // Tu pool local en CUIPO

// ⚙️ Función auxiliar local para obtener usuario por email
const getUserByEmail = async (email) => {
  const query = `
    SELECT
      u.id_user,
      u.name_user,
      u.email_user,
      u.id_role_user,
      u.id_dependency_user,
      r.rol_name,
      d.dependency_name AS user_secretaria_name,
      u.id_programm_user
    FROM "sis_catastro_verificacion"."tbl_users" u
    JOIN "sis_catastro_verificacion"."tbl_role" r ON u.id_role_user = r.id_role
    JOIN "sis_catastro_verificacion"."tbl_dependency" d ON u.id_dependency_user = d.id_dependency
    WHERE u.email_user = $1
  `;
  const { rows } = await pool.query(query, [email]);
  return rows[0];
};

const verifyTokenInfo = async (req, res, next) => {
  try {
    const token = req.headers.authorization?.split(' ')[1];
    if (!token) return res.status(401).json({ error: 'Token no proporcionado' });

    const decoded = jwt.verify(token, process.env.JWT_SECRET);
    const user = await getUserByEmail(decoded.email);
    if (!user) return res.status(401).json({ error: 'Usuario no encontrado' });

    req.user = {
      id: user.id_user,
      email: user.email_user,
      name: user.name_user,
      role: user.rol_name,
      id_role_user: user.id_role_user,
      id_dependency_user: user.id_dependency_user,
      dependencyName: user.user_secretaria_name,
      program: user.id_programm_user
    };

    next();
  } catch (err) {
    console.error('Error en verifyTokenInfo middleware:', err);
    return res.status(401).json({ error: 'Token inválido o expirado' });
  }
};

const verifyToken = async (req, res) => {
  try {
    console.log('\n=== INICIO DE VERIFICACIÓN ===');
    console.log('Headers recibidos:', req.headers);
   
    const token = req.headers.authorization?.split(' ')[1];
    console.log('Token recibido:', token);
    console.log('authController.js - Token recibido (truncado):', token ? token.substring(0, 30) + '...' : 'No token');
   
    if (!token) {
      console.log('Error: Token no proporcionado');
      return res.status(401).json({
        valid: false,
        error: 'Token no proporcionado',
        details: 'No se encontró token en headers Authorization'
      });
    }

    const decoded = jwt.verify(token, process.env.JWT_SECRET);
    console.log('Token decodificado (payload):', decoded);
   
    if (!decoded.id || !decoded.email) {
      console.log('Error: Token inválido - Faltan campos esenciales');
      return res.status(401).json({
        valid: false,
        error: 'Token inválido',
        details: 'El token no contiene los campos requeridos'
      });
    }

    // Obtenemos la información del usuario directamente de la base de datos
    const user = await getUserByEmail(decoded.email);
    console.log('authController.js - Usuario encontrado en BD por email:', user);
   
    if (!user) {
      console.log('Error: Usuario no encontrado en BD');
      return res.status(401).json({
        valid: false,
        error: 'Usuario no encontrado',
        details: 'El usuario del token no existe en la base de datos'
      });
    }

    // --- INICIO DE LOS CAMBIOS CLAVE EN verifyToken (DEFINITIVOS) ---

    // Log de depuración, usando las propiedades CORRECTAS del objeto 'user'
    console.log('authController.js - Preparando respuesta de usuario con dependencyName (de BD):', user.user_secretaria_name);
    console.log('authController.js - Preparando respuesta de usuario con program (id_programm_user de BD):', user.id_programm_user);


    // Adjuntar toda la información relevante del usuario a 'req.user' para el middleware
    req.user = {
      id: user.id_user,
      email: user.email_user,
      role: user.rol_name,
      roleName: user.rol_name,
      id_role_user: user.id_role_user,
      id_dependency_user: user.id_dependency_user,
      name: user.name_user,
      program: user.id_programm_user, // Tomamos el 'program' del usuario de la BD
      dependencyName: user.user_secretaria_name // Tomamos el nombre de la secretaría de la BD
    };

    console.log('=== VERIFICACIÓN EXITOSA ===');

    // Construcción del objeto 'user' para la respuesta JSON
    // Usamos las propiedades DIRECTAMENTE del objeto 'user' obtenido de la BD
    res.json({
      valid: true,
      user: {
        id: user.id_user,
        email: user.email_user,
        role: user.rol_name,
        id_role_user: user.id_role_user,
        name: user.name_user,
        program: user.id_programm_user, // ¡CLAVE: Tomamos el programa del usuario de la BD!
        dependencyName: user.user_secretaria_name // ¡CLAVE: Tomamos el nombre de la secretaría de la BD!
      },
      tokenInfo: {
        issuedAt: new Date(decoded.iat * 1000),
        expiresAt: new Date(decoded.exp * 1000),
        program: decoded.program // Esto es el 'program' original del token (puede ser redundante, pero lo mantengo por si acaso)
      }
    });

  } catch (error) {
    console.error('Error en verificación:', error);
   
    const response = {
      valid: false,
      error: 'Error de autenticación',
      details: error.message
    };
   
    if (error.name === 'TokenExpiredError') {
      response.error = 'Token expirado';
      response.expiredAt = error.expiredAt;
    } else if (error.name === 'JsonWebTokenError') {
      response.error = 'Formato de token inválido';
    }

    res.status(401).json(response);
  }
};

module.exports = {
  verifyToken,
  verifyTokenInfo
}
