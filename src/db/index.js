// src/db/index.js
const { Pool } = require("pg");
require("dotenv").config();

const pool = new Pool({
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  host: process.env.DB_HOST,
  port: process.env.DB_PORT,
  database: process.env.DB_NAME,
});

async function testDBConnection() {
  try {
    const client = await pool.connect();
    await client.query('SET search_path TO ' + process.env.DB_SCHEMA);
    console.log("✅ Conexión exitosa a la base de datos PostgreSQL");
    client.release();
  } catch (err) {
    console.error("❌ Error al conectar con la base de datos:", err.message);
    process.exit(1); // Finaliza el proceso si falla la conexión
  }
}

module.exports = { pool, testDBConnection };