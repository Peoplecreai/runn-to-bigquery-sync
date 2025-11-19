# Solución al Problema de Duplicación 2.6x en PowerBI

## 📊 Resumen del Problema

**Síntomas:**
- Dashboard de PowerBI reporta 2.6x más horas de las reales
- Ejemplo: Marcela Aburto semana 10-14 Nov 2025
  - Real (Clockify): 40 horas (23 entries)
  - PowerBI reporta: 103 horas
  - Factor: 2.575x

## 🔍 Causa Raíz Identificada

El problema tiene **DOS niveles de duplicación**:

### 1. Duplicados en la API de Clockify
La API de Clockify devuelve el MISMO time entry múltiples veces cuando se itera por usuarios:
- Un time entry puede aparecer 2-3 veces si está asociado con múltiples usuarios
- Esto causaba que el mismo registro se procesara múltiples veces

**Status:** ✅ **YA SOLUCIONADO** en `clockify_client.py:98-100`

### 2. Duplicados Históricos en BigQuery
Los duplicados de ejecuciones ANTERIORES (antes de implementar la deduplicación en nivel 1) se acumularon en BigQuery:
- El MERGE usaba `id` (hash numérico) como clave única
- Si había múltiples rows con el mismo `_clockify_id` pero diferentes `id`, no se detectaban como duplicados
- Los MERGE sucesivos NO eliminaban estos duplicados históricos

**Status:** 🔴 **REQUIERE LIMPIEZA** - este es el problema actual

## ✅ Solución Implementada

### Cambios en el Código

#### 1. **bq_utils.py** - MERGE mejorado
- **Antes:** MERGE por `id` (hash numérico)
- **Ahora:** MERGE por `_clockify_id` (ID real de Clockify)
- Agregado: Deduplicación automática del staging antes del merge
- Agregado: Función `deduplicate_table_by_column()` para limpiar duplicados históricos

**Archivo:** `bq_utils.py:47-75`

#### 2. **main.py** - Sincronización con limpieza automática
- **Antes:** Solo hacía MERGE sin verificar duplicados
- **Ahora:**
  - Limpia duplicados históricos ANTES del merge
  - Usa `_clockify_id` como clave única para Clockify
  - Deduplica automáticamente en cada sincronización

**Archivo:** `main.py:103-114`

#### 3. **fix_duplicates_now.py** - Script de limpieza inmediata
- Script standalone para limpiar duplicados AHORA MISMO
- No requiere esperar a la próxima sincronización
- Reporta estadísticas antes/después

### Protecciones Implementadas

El pipeline ahora tiene **3 capas de protección** contra duplicados:

1. **Capa 1 - API de Clockify** (`clockify_client.py:68-121`)
   - Deduplica por Clockify ID al obtener datos de la API
   - Reporta estadísticas de duplicados detectados

2. **Capa 2 - Transformación** (`main.py:53-89`)
   - Verifica IDs duplicados después de transformar
   - Detecta colisiones de hash MD5

3. **Capa 3 - BigQuery** (`main.py:103-114`, `bq_utils.py:61-69`)
   - Limpia duplicados históricos antes del merge
   - Deduplica staging antes del merge
   - Usa `_clockify_id` como clave única

## 🚀 Cómo Aplicar el Fix

### Opción 1: Limpieza Inmediata (RECOMENDADO)

Ejecuta el script de limpieza inmediata:

```bash
# Configurar credenciales de BigQuery
export BQ_PROJECT="tu-proyecto-gcp"
export BQ_DATASET="people_analytics"

# Ejecutar limpieza
python fix_duplicates_now.py
```

**Resultado esperado:**
```
⚠️  Duplicados detectados en project.people_analytics.runn_actuals:
   Total rows: 60 (ejemplo)
   Rows únicos: 23
   Duplicados a eliminar: 37
   Factor de duplicación: 2.61x

✅ Deduplicación completada: 60 → 23 rows
```

### Opción 2: Full Sync

Alternativamente, puedes hacer un full sync que borra todo y recarga desde cero:

```bash
export FULL_SYNC=true
python main.py
```

⚠️ **Advertencia:** Esto borrará TODA la tabla y recargará todos los datos de Clockify (últimos 90 días)

### Opción 3: Sincronización Normal

La próxima sincronización normal ya incluye limpieza automática:

```bash
python main.py
```

El script automáticamente:
1. Detectará duplicados existentes
2. Los limpiará antes del merge
3. Usará `_clockify_id` para evitar duplicados futuros

## 🧪 Validación

### 1. Verificar limpieza en BigQuery

```sql
-- Contar registros para Marcela Aburto (semana 10-14 Nov 2025)
SELECT
    COUNT(*) as total_registros,
    COUNT(DISTINCT _clockify_id) as clockify_ids_unicos,
    SUM(billableMinutes) / 60.0 as billable_hours,
    SUM(nonbillableMinutes) / 60.0 as nonbillable_hours,
    (SUM(billableMinutes) + SUM(nonbillableMinutes)) / 60.0 as total_hours
FROM `project.people_analytics.runn_actuals`
WHERE date BETWEEN '2025-11-10' AND '2025-11-14'
  AND personId IN (
    SELECT id FROM `project.people_analytics.runn_people`
    WHERE LOWER(firstName || ' ' || lastName) LIKE '%marcela%aburto%'
  )
```

**Esperado:**
- `total_registros` = `clockify_ids_unicos` = 23
- `total_hours` ≈ 40.0 horas

### 2. Verificar en PowerBI

1. Refresca tu dataset de PowerBI
2. Verifica las horas de Marcela Aburto (10-14 Nov 2025)
3. Debería mostrar ~40 horas (no 103)

### 3. Script de debugging

Usa el script de debugging existente para análisis detallado:

```bash
python debug_duplicates.py
```

Este script verifica:
- Conteo de registros vs IDs únicos
- Suma de horas en BigQuery
- Duplicados por ID
- Colisiones de hash
- Distribución por fecha

## 📋 Prevención Futura

Los cambios implementados previenen duplicados futuros automáticamente:

✅ **Deduplicación en API** - Ya no se obtienen duplicados de Clockify
✅ **MERGE por Clockify ID** - Usa el ID real como clave única
✅ **Limpieza automática** - Elimina duplicados históricos en cada sync
✅ **Deduplicación de staging** - Verifica staging antes del merge

**No se requiere acción adicional** - el pipeline ya está corregido.

## 🔧 Archivos Modificados

1. `bq_utils.py`
   - Nueva función: `deduplicate_table_by_column()`
   - MERGE mejorado con deduplicación automática
   - Soporte para claves únicas custom (no solo `id`)

2. `main.py`
   - Limpieza de duplicados antes del merge para Clockify
   - Uso de `_clockify_id` como clave única
   - Import de `deduplicate_table_by_column`

3. `fix_duplicates_now.py` (NUEVO)
   - Script de limpieza inmediata
   - Reporta estadísticas antes/después

4. `SOLUTION_DUPLICADOS.md` (NUEVO)
   - Este documento de solución completa

## ❓ FAQ

### ¿Por qué 2.6x específicamente?

Si la API de Clockify devuelve cada time entry ~2.6 veces en promedio (algunos 2x, otros 3x), y estos duplicados se acumularon en múltiples sincronizaciones, el factor de duplicación final es ~2.6x.

### ¿Se perderán datos al limpiar duplicados?

No. La deduplicación mantiene el registro MÁS RECIENTE (por `updatedAt`) de cada time entry único. Solo elimina copias exactas del mismo time entry.

### ¿Afectará esto a mis reportes históricos?

Sí, en el sentido de que **corregirá** los números. Los reportes históricos que antes mostraban 2.6x más horas ahora mostrarán las horas correctas.

### ¿Necesito ejecutar el fix cada vez?

No. Solo necesitas ejecutar `fix_duplicates_now.py` UNA VEZ para limpiar duplicados históricos. Después, el pipeline normal (`main.py`) ya previene duplicados futuros automáticamente.

### ¿Qué pasa si vuelvo a ejecutar una sincronización antigua?

El MERGE ahora usa `_clockify_id` como clave única, por lo que aunque ejecutes el mismo time entry múltiples veces, solo se actualizará (no se duplicará).

## 📞 Soporte

Si después de aplicar el fix sigues viendo duplicados:

1. Ejecuta `python debug_duplicates.py` para análisis detallado
2. Verifica que las variables de entorno estén configuradas correctamente
3. Revisa los logs de la sincronización para errores
4. Contacta al equipo de desarrollo con los logs

---

**Fecha de solución:** 2025-11-19
**Autor:** Claude Code
**Estado:** ✅ Listo para implementación
