# Bayer Analytics App

El frontend usa React + Vite + Tailwind en modo dark y el backend expone una API en FastAPI preparada para operar contra BigQuery.

## Estructura del proyecto

```
project/
 backend/      # FastAPI + integracion BigQuery
 frontend/     # React + Vite + Tailwind + React Router
```

## Backend (FastAPI)

1. **Crear y activar entorno virtual**
   ```bash
   cd backend
   python -m venv .venv
   .venv\Scripts\activate
   ```
2. **Instalar dependencias**
   ```bash
   pip install -r requirements.txt
   ```
3. **Configurar variables de entorno**
   - Copia `.env.example` a `.env` (en la raiz del proyecto) y completa segun tu entorno:
     ```ini
     APP_PASSWORD=bayern2025
     GCP_PROJECT_ID=your-project-id
     BIGQUERY_LOCATION=us-central1
     BIGQUERY_CREDENTIALS_PATH=./path/to/service-account.json

     # Dataset Instar (override opcional de proyecto)
     INSTAR_PROJECT_ID=
     INSTAR_DATASET=bayer
     INSTAR_TABLE=instar_historico
     INSTAR_MONTH_COLUMN=Mes_Anio

     # Dataset AdMedia (override opcional de proyecto)
     ADMEDIA_PROJECT_ID=
     ADMEDIA_DATASET=bayer
     ADMEDIA_TABLE=admedia_historico
     ADMEDIA_MONTH_COLUMN=Mes
     ```
   - Si `INSTAR_PROJECT_ID` o `ADMEDIA_PROJECT_ID` quedan vacios, se usa `GCP_PROJECT_ID`.
   - `APP_PASSWORD` controla el login basico (`POST /api/login`).
4. **Ejecutar el backend**
   ```bash
   uvicorn app.main:app --reload
   ```
   El API queda disponible en `http://127.0.0.1:8000/api`.

### Endpoints principales

- `POST /api/login` valida la contraseña recibida contra `APP_PASSWORD`.
- `GET /api/instar/meses` y `GET /api/admedia/meses` consultan BigQuery para listar meses disponibles ya ordenados segun el formato heredado.
- `DELETE /api/instar` y `DELETE /api/admedia` eliminan filas para los meses enviados (usa consultas parametrizadas `IN UNNEST(@months)`).
- `POST /api/instar/append` y `POST /api/admedia/append` ejecutan `INSERT ... SELECT` entre tablas BigQuery (con filtro opcional por meses).
- `POST /api/export` ejecuta la consulta y devuelve un CSV listo para descargar (maximo 3 meses por request).

Los helpers de BigQuery se ubican en `backend/app/services/bigquery_service.py` y reutilizan la logica de formatos y normalizacion que tenia el script de Streamlit. Ajusta ahi cualquier detalle de tablas/columnas adicionales.

## Frontend (React + Vite)

1. **Instalar dependencias**
   ```bash
   cd frontend
   npm install
   ```
2. **Levantar el servidor de desarrollo**
   ```bash
   npm run dev
   ```
   La app se sirve en `http://localhost:5173` y el proxy de Vite reenvia `/api/*` hacia `http://127.0.0.1:8000`.

### Puntos clave del frontend

- React Router define `/login` y `/dashboard`.
- `AuthContext` guarda el flag de sesion en `localStorage`.
- `SelectionContext` comparte meses disponibles/seleccionados entre pestanas.
- Tailwind CSS aporta el dark mode.
- Dashboard con pestanas Instar, AdMedia y Export: fetch de meses, borrado, append y export usando la nueva API.





