# 3. Reglas de calidad, identificación y actualización

## Limpieza (4.3.2 y 4.3.3)
- **HTML**: se eliminan etiquetas de título y descripción, incluido HTML doblemente escapado
  (`&lt;p&gt;`), se descartan `<script>`/`<style>`, se decodifican entidades (`&eacute;` → é),
  se normaliza Unicode (NFC) y los espacios. Se conserva el original en `*_raw`.
- **Fechas**: se aceptan ISO 8601 y RFC 822; se convierten a UTC (`pub_date_utc`) y se conserva el
  texto original (`pub_date_raw`). Sin zona horaria se asume UTC.

## Campos obligatorios (4.3.4)
| Regla | Motivo de rechazo |
|---|---|
| Título no vacío después de limpiar HTML | `MISSING_TITLE` |
| Enlace presente | `MISSING_LINK` |
| Enlace `http`/`https` con dominio | `INVALID_LINK` |
| Fecha presente | `MISSING_PUB_DATE` |
| Fecha interpretable | `INVALID_PUB_DATE` |
| No más de 24 h en el futuro respecto a la extracción | `FUTURE_PUB_DATE` |
| No anterior al año 2000 | `IMPLAUSIBLE_PUB_DATE` |

Un registro puede tener varios motivos (`reasons`). Los inválidos van a
`quarantine/silver/.../rejected.parquet` con la referencia a su archivo y lote Bronze (4.3.8).

## Estrategia de identificación
`news_id = sha256(clave)[:32]`, con la clave elegida en este orden:

1. **guid con forma de URL** → URL canónica del guid (`guid_url`). Es el caso de El Tiempo.
2. **guid opaco** → `guid:<medio>:<guid>` (`guid`); se antepone el medio porque un guid opaco solo es único dentro de su emisor.
3. **sin guid** → URL canónica del enlace (`link`).
4. **sin guid ni enlace** → `hash:<medio>|<título normalizado>|<fecha UTC>` (`title_date_hash`). Queda definida, aunque en la práctica no se usa porque el enlace es obligatorio.

**URL canónica**: esquema `https`, host en minúsculas, sin puerto por defecto, sin `#fragmento`,
sin parámetros de seguimiento (`utm_*`, `fbclid`, `gclid`…), parámetros restantes ordenados y sin
`/` final. Así `http://WWW.eltiempo.com/x/?utm_source=rss` y `https://www.eltiempo.com/x` son la
misma noticia.

## Repetidas dentro de un lote y entre lotes (4.3.5)
- **Dentro de un lote**: se conserva la primera aparición; las demás se cuentan en `duplicates_in_batch`.
- **Entre lotes**: el mismo `news_id` se fusiona con la noticia vigente (`unchanged` si nada cambió).

## Noticias presentes en varios feeds
Una nota que aparece en Ciencia y en Medio Ambiente tiene el mismo `news_id`, por lo que queda **una
sola fila** en `silver_news`:
- `first_source_id` / `first_seccion`: el primer feed donde se vio.
- `source_ids` / `secciones` / `n_sources`: todos los feeds donde ha aparecido.
- En el lote del segundo feed se cuenta como `cross_feed_matches`, no como noticia nueva.
- En Gold, `news_count` la cuenta en cada sección (responde "¿cuántas noticias tuvo la sección?") y
  `news_count_first_section` una sola vez (responde "¿cuántas noticias hubo?").

## Limitaciones de la estrategia
- La **misma nota con otra URL** (otra ruta de sección, slug editado tras publicar) produce dos
  `news_id`. Para detectarlo está `title_fingerprint` (consulta `athena/queries/19_posibles_duplicados_por_titulo.sql`), pero no se fusionan
  automáticamente para no unir notas distintas con el mismo título (p. ej. "Pico y placa hoy").
- **Cambiar la URL** de una nota se ve como noticia nueva, no como actualización.
- Si un medio usara un **parámetro de query como identificador**, se conserva; solo se eliminan
  los de seguimiento conocidos.
- El mismo artículo en **dos medios distintos** (contenido de agencia) no se detecta: son URLs distintas.
- Si el medio cambia el `guid` de una nota sin cambiar la URL, se generará otro `news_id`.

## Cambios en noticias ya procesadas (4.3.6)
- `content_hash` = huella de título + descripción + URL canónica + fecha UTC.
- Si cambia: `version + 1`, se actualizan los campos, `bronze_run_id/uri` pasan al lote nuevo y se
  escribe una fila `UPDATED` en `silver_news_history` con `changed_fields` y `text_changed`.
- Un lote **más antiguo** que la versión vigente (p. ej. reprocesado tarde) no la pisa: se cuenta
  como `stale`.
- Comprehend se vuelve a llamar **solo si cambia el texto analizado** (título + descripción). Un
  cambio solo de fecha o de enlace crea versión nueva pero reutiliza el análisis en caché.

## Enriquecimiento con Comprehend
- Texto analizado = título + descripción limpios. Se guarda el texto, su hash, el idioma, la fecha
  de análisis y las puntuaciones (`silver_nlp_analysis`).
- **Sin llamadas repetidas**: caché por `text_hash`. Se usan las APIs Batch (25 documentos por
  llamada): `BatchDetectDominantLanguage`, `BatchDetectEntities`, `BatchDetectKeyPhrases`.

| Estado | Cuándo | ¿Se guarda en caché? |
|---|---|---|
| `ANALYZED` | análisis completo | sí |
| `ANALYZED_TRUNCATED` | texto > 4.900 bytes (límite del servicio: 5 KB por documento); se analiza el inicio cortado en palabra | sí |
| `SKIPPED_EMPTY` | menos de 3 caracteres | sí |
| `SKIPPED_UNSUPPORTED_LANGUAGE` | idioma fuera de `[es, en]` | sí |
| `PENDING_QUOTA` | supera `max_documents_per_run` (2.000), tope de costo | no: sigue en la próxima ejecución |
| `ERROR` | error del servicio tras los reintentos | no: se reintenta en la próxima ejecución |

Si la detección de idioma no es confiable (score < 0,5, típico en textos muy cortos) se usa el
idioma del feed (`es`) y queda `language_source = default`.

## Validaciones (4.5)
Hay dos niveles:
1. **Compuerta antes de publicar** (Python, en `silver_publish` y `gold_build`): unicidad de
   `news_id`, obligatorios, entidades/frases sin noticia, entidad repetida en una noticia y
   conciliación Silver → Gold por partición. Si falla, **no se publica**.
2. **Validaciones finales con Athena** (`athena_validate`, `athena_checks.py`), sobre lo que ya ve
   Athena. Resultados en la tabla `quality_check_results`:

| check_name | Valida |
|---|---|
| news_id_unico | Unicidad del identificador en el conjunto vigente |
| campos_obligatorios | Presencia de campos obligatorios |
| fechas_validas | Fecha válida y coherente con la partición |
| enlaces_validos | Enlace http(s) |
| entidades_sin_noticia / frases_sin_noticia | Ausencia de entidades o frases sin noticia |
| entidad_distinta_por_noticia | Granularidad de `silver_news_entities` |
| conciliacion_noticias / _entidades / _frases | Gold concilia con Silver |
| cuarentena_trazable | Todo rechazo tiene motivo y lote Bronze |
| conteos_por_lote_cuadran | `recibidos = inválidos + repetidos + nuevos + actualizados + sin cambio + obsoletos` |

## Registros recibidos vs. noticias únicas publicadas
Por cada lote (`silver_batch_log`):

```
items_read = invalid + duplicates_in_batch + new + updated + unchanged + stale
```

Las noticias únicas publicadas crecen solo con `new`. La diferencia con los registros recibidos se
explica porque: (a) los feeds repiten durante días las mismas notas (`unchanged`), (b) una nota
puede venir repetida dentro del mismo feed (`duplicates_in_batch`), (c) una nota puede aparecer en
varios feeds (`cross_feed_matches`, ya contada como `unchanged` o `updated`), y (d) los inválidos
van a cuarentena (`invalid`).
