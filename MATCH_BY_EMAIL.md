# Match de Datos por Email entre Clockify y Runn

## Problema Identificado

Los datos de Clockify no incluyen el ID que tenía Runn, lo que causaba inconsistencias al tratar de relacionar los datos de ambas fuentes. Anteriormente, el sistema generaba IDs numéricos determinísticos a partir de los IDs de Clockify, pero esto no garantizaba que el `personId` coincidiera con el ID real de la persona en Runn.

## Solución Implementada

Se implementó un sistema de match por email entre Clockify y Runn, usando el email como campo de unión. Esto garantiza que los datos de time entries de Clockify se asocien correctamente con las personas en Runn.

## Flujo de Datos

```
┌─────────────────────────────────────────────────────────────────┐
│                    FLUJO DE MATCH POR EMAIL                      │
└─────────────────────────────────────────────────────────────────┘

1. Obtener usuarios de Clockify
   ↓
   fetch_all_users() → [{id, email, name, ...}]
   ↓
2. Construir mapeo: Clockify userId → email
   ↓
   build_user_email_map() → {clockify_userId: email}
   ↓
3. Obtener personas de Runn
   ↓
   fetch_all("/people/") → [{id, email, firstName, lastName, ...}]
   ↓
4. Construir mapeo completo: Clockify userId → Runn personId
   ↓
   build_user_map_by_email() → {clockify_userId: runn_personId}
   ↓
5. Transformar time entries usando el mapeo
   ↓
   transform_batch() con user_map → [{...actuals con personId correcto...}]
   ↓
6. Cargar a BigQuery con personId correcto
```

## Archivos Modificados

### 1. `clockify_client.py`

**Nueva función:**
- `build_user_email_map()`: Construye un mapeo de userId de Clockify a email

```python
def build_user_email_map():
    """
    Construye un mapeo de userId de Clockify a email.

    Returns:
        dict: {clockify_userId: email}
    """
```

### 2. `clockify_transformer.py`

**Nuevas funciones:**

- `build_user_map_by_email()`: Construye mapeo completo usando email como puente

```python
def build_user_map_by_email(
    clockify_user_email_map: Dict[str, str],
    runn_people: List[Dict[str, Any]]
) -> tuple[Dict[str, int], Dict[str, str]]:
    """
    Construye mapeo de userId de Clockify → personId de Runn usando email.

    Returns:
        tuple: (user_map, match_stats)
    """
```

**Funciones modificadas:**

- `transform_time_entry_to_actual()`: Ahora acepta `clockify_user_email_map` para auditoría
- `transform_batch()`: Ahora acepta `clockify_user_email_map` para auditoría

**Nuevos campos en actuals:**

- `_clockify_user_email`: Email del usuario para auditoría
- `_clockify_matched_by_email`: Boolean que indica si el match fue exitoso por email

### 3. `main.py`

**Modificaciones en `sync_actuals_from_clockify()`:**

1. Obtiene personas de Runn: `fetch_all("/people/")`
2. Construye mapeo de Clockify: `build_user_email_map()`
3. Construye mapeo completo: `build_user_map_by_email()`
4. Imprime estadísticas detalladas del match
5. Pasa mapeos a `transform_batch()`

## Estadísticas de Match

Al ejecutar el sync, el sistema ahora muestra estadísticas detalladas:

```
============================================================
📊 ESTADÍSTICAS DE MATCH POR EMAIL:
============================================================
  Usuarios en Clockify: 45
  Personas en Runn: 42
  Matches exitosos: 40
  Sin match: 5
  Tasa de match: 88.9%

  ⚠️  Usuarios de Clockify sin match en Runn:
     - usuario1@example.com (Clockify ID: abc123)
     - usuario2@example.com (Clockify ID: def456)
     ...
============================================================
```

## Campos de Auditoría

Los datos cargados a BigQuery ahora incluyen campos adicionales para auditoría:

| Campo | Tipo | Descripción |
|-------|------|-------------|
| `_clockify_user_email` | STRING | Email del usuario de Clockify |
| `_clockify_matched_by_email` | BOOLEAN | Si el match fue exitoso por email |

Estos campos permiten:
- Identificar qué usuarios no tienen match entre Clockify y Runn
- Auditar la calidad del match
- Investigar discrepancias en los datos

## Comportamiento de Fallback

Si un usuario de Clockify no tiene match en Runn por email:
1. El sistema genera un `personId` determinístico usando MD5 hash (comportamiento anterior)
2. El campo `_clockify_matched_by_email` se marca como `false`
3. Se registra en las estadísticas como "sin match"

Esto garantiza que todos los time entries se procesen, incluso si no hay match perfecto.

## Ventajas del Nuevo Sistema

1. **Consistencia**: Los personId ahora son consistentes con los de Runn
2. **Trazabilidad**: Los campos de auditoría permiten rastrear el origen de cada dato
3. **Transparencia**: Las estadísticas muestran claramente la calidad del match
4. **Flexibilidad**: El sistema funciona incluso si algunos usuarios no tienen match
5. **Mantenibilidad**: Es fácil identificar y corregir problemas de datos

## Uso

No se requieren cambios en la configuración. El sistema usa automáticamente el match por email:

```bash
# Sync normal
python main.py

# Full sync (recarga completa)
FULL_SYNC=true python main.py
```

## Troubleshooting

### Baja tasa de match

Si la tasa de match es baja (<80%), verificar:
1. Que los emails en Clockify y Runn sean consistentes (case-insensitive)
2. Que los usuarios estén activos en ambos sistemas
3. Que no haya typos en los emails

### Usuarios sin match

Para usuarios sin match, opciones:
1. Corregir el email en uno de los sistemas
2. Crear la persona en Runn si no existe
3. Aceptar el personId generado determinísticamente

## Próximos Pasos

Posibles mejoras futuras:
1. Match por nombre si el email falla
2. API para actualizar emails automáticamente
3. Dashboard de auditoría de matches
4. Notificaciones cuando la tasa de match es baja
