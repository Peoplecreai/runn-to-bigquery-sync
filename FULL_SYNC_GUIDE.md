# Guía de Full Sync - Solución de Duplicados en BigQuery

## 🔍 Problema Identificado

### Síntoma
Los datos en BigQuery se estaban duplicando. Por ejemplo, una empleada que registró 40 horas en Clockify aparecía con 102 horas en PowerBI.

### Causa Raíz
El sistema usaba la función `hash()` de Python para generar IDs numéricos a partir de los IDs de Clockify. El problema es que **`hash()` no es determinístico** en Python 3.3+:

- Al reiniciar el servicio, la misma entrada de tiempo generaba un ID diferente
- El MERGE en BigQuery no reconocía el registro como existente
- Se insertaba como nuevo en lugar de actualizar → **duplicados masivos**

## ✅ Solución Implementada

### 1. Hash Determinístico (MD5)
**Archivo modificado:** `clockify_transformer.py`

Se reemplazó `hash()` por `hashlib.md5()` que es determinístico:

```python
# ANTES (problemático):
numeric_id = abs(hash(clockify_id)) % (10**10)

# DESPUÉS (correcto):
numeric_id = _generate_deterministic_id(clockify_id)
```

**Resultado:** El mismo time entry de Clockify siempre genera el mismo ID, sin importar cuántas veces se reinicie el servicio.

### 2. Mecanismo de Full Sync
**Archivos modificados:** `main.py`, `bq_utils.py`

Se añadió soporte para truncar (borrar) las tablas antes de recargar, eliminando todos los duplicados existentes.

## 🚀 Cómo Ejecutar el Full Sync

### Opción 1: Variable de Entorno (Recomendado)

```bash
# Activar FULL_SYNC para limpiar duplicados
FULL_SYNC=true python main.py
```

O si usas Docker/Cloud Run:

```bash
# En .env o configuración del servicio
FULL_SYNC=true
```

### Opción 2: Configuración Temporal

```bash
# Ejecutar una sola vez con full sync
export FULL_SYNC=true
python main.py

# Desactivar para siguientes ejecuciones
unset FULL_SYNC
```

### Opción 3: Cloud Run / Kubernetes

Actualizar la variable de entorno en la configuración del servicio:

```yaml
env:
  - name: FULL_SYNC
    value: "true"
```

## ⚠️ Advertencias Importantes

1. **El Full Sync borra TODAS las tablas configuradas** en `endpoints.yaml` antes de recargar
2. **Solo ejecutar cuando sea necesario** (después de desplegar el fix del hash, o para limpiar duplicados)
3. **Tarda más tiempo** que un sync normal porque recarga todo el histórico
4. **Después del full sync, desactivar FULL_SYNC** para volver a syncs incrementales

## 📊 Proceso del Full Sync

```
1. Detectar FULL_SYNC=true
   ↓
2. Para cada tabla en endpoints.yaml:
   ├── Truncar tabla target (borrar todo)
   ├── Obtener datos de Clockify/Runn
   ├── Cargar a tabla staging
   └── MERGE (INSERT todo, ya que target está vacío)
   ↓
3. Resultado: Datos limpios sin duplicados
```

## 🔧 Flujo Recomendado para Limpiar Duplicados

### Paso 1: Hacer Full Sync (UNA VEZ)
```bash
# Ejecutar con full sync para limpiar duplicados
FULL_SYNC=true python main.py
```

Verás este mensaje:
```
============================================================
⚠️  FULL SYNC ACTIVADO - Se borrarán todas las tablas antes de recargar
============================================================

[runn_actuals] FULL SYNC activado - truncando tabla...
[runn_actuals] full sync: 2000 filas desde Clockify

✅ Full sync completado - Todos los duplicados han sido eliminados
```

### Paso 2: Desactivar Full Sync
```bash
# Volver a modo normal (solo para siguientes ejecuciones)
unset FULL_SYNC
```

O remover la variable de tu configuración en Cloud Run.

### Paso 3: Verificar en PowerBI
- Las horas ahora deben coincidir con Clockify
- No debe haber duplicados

## 🔄 Syncs Futuros (Modo Normal)

Después del full sync inicial, los syncs normales funcionarán correctamente gracias al hash determinístico:

```bash
# Sin FULL_SYNC (modo normal)
python main.py

# Output esperado:
[runn_actuals] upsert: 2000 filas desde Clockify
```

El MERGE ahora funciona correctamente:
- Actualiza registros existentes
- Inserta solo los nuevos
- **Sin duplicados**

## 📝 Notas Técnicas

### ¿Por qué MD5 en lugar de otro hash?

- **Determinístico**: Siempre genera el mismo hash para el mismo input
- **Rápido**: Suficiente para generar IDs
- **Ampliamente soportado**: Disponible en todas las versiones de Python
- **No es para seguridad**: Solo para generar IDs únicos consistentes

### Tabla de Cambios

| Archivo | Cambio | Propósito |
|---------|--------|-----------|
| `clockify_transformer.py` | `hash()` → `hashlib.md5()` | IDs determinísticos |
| `bq_utils.py` | Añadir `truncate_table()` | Limpiar tablas |
| `main.py` | Añadir soporte `FULL_SYNC` | Control de full sync |

## 🆘 Troubleshooting

### Los duplicados siguen apareciendo
- Verificar que el código actualizado esté desplegado
- Ejecutar full sync con `FULL_SYNC=true`
- Verificar que no haya múltiples procesos escribiendo a BigQuery

### El full sync falla
- Verificar permisos en BigQuery (necesita TRUNCATE TABLE)
- Revisar logs para errores específicos
- Verificar que las tablas existan

### ¿Cuándo usar full sync?
- **Después de desplegar el fix del hash** (primera vez)
- **Si detectas duplicados** en los datos
- **Si cambias la lógica de IDs** en el transformer
- **NO usar en syncs regulares** (más lento e innecesario)

## 📞 Contacto

Si encuentras problemas, revisa los logs y verifica:
1. Código actualizado desplegado
2. FULL_SYNC activado correctamente
3. Permisos en BigQuery
4. Sin otros procesos escribiendo simultáneamente
