const express = require("express");
const cors = require("cors");
const bodyParser = require('body-parser');
require("dotenv").config();

const { testDBConnection } = require("./db");
const cuipoRoutes = require("./routes/cuipoRoutes");

const app = express();
const PORT = process.env.PORT || 5005;

// Aumentar el límite a 50MB (ajusta según necesites)
app.use(bodyParser.json({ limit: '50mb' }));
app.use(bodyParser.urlencoded({ limit: '50mb', extended: true }));

app.use(cors({
  origin: function (origin, callback) {
    const allowedOrigins = [
      'http://10.125.126.107:3005', // tu IP en red
      'http://localhost:3005',
    ];
    // Permitir solicitudes sin "origin" (como Postman)
    if (!origin || allowedOrigins.includes(origin)) {
      callback(null, true);
    } else {
      callback(new Error('Not allowed by CORS'));
    }
  },
  credentials: true,
}));

app.use(express.json());

app.use("/api/v1/cuipo", cuipoRoutes);

app.get("/", (req, res) => {
  res.send("✅ Servidor CUIPO corriendo correctamente");
});

app.listen(PORT, '0.0.0.0', async () => {
  console.log(`🚀 Servidor corriendo en http://0.0.0.0:${PORT}`);
  await testDBConnection();
});
