# 6. Limitaciones del enriquecimiento de texto

Para la demostración se pide explicar al menos una. La principal:

## 1. Textos muy cortos: solo título y descripción del RSS
El RSS de El Tiempo trae el título y un resumen de una o dos frases; no se descarga el artículo
completo. Con tan poco contexto, Comprehend:
- **Omite entidades** que solo aparecen en el cuerpo de la nota. Una noticia sobre un acuerdo
  puede no tener ninguna organización detectada si el resumen no la nombra.
- **Clasifica mal algunas menciones**: un apellido que también es lugar, una institución con nombre
  de persona ("Fundación Santa Fe"), o siglas sin contexto pueden salir como el tipo equivocado
  o con confianza baja (`avg_score`).
- **Detecta mal el idioma** en títulos cortos; por eso, si la confianza es menor a 0,5 se usa el
  idioma del feed (`language_source = default`).

Consecuencia para los indicadores: `gold_entity_daily` mide **menciones detectadas
automáticamente en títulos y resúmenes**, no la presencia real de la entidad en la noticia.

## Otras limitaciones
- **Sin resolución de alias**: "Petro", "Gustavo Petro" y "el presidente Petro" son entidades
  distintas; solo se unifican mayúsculas y tildes (`entity_text_norm`). El enunciado no lo exige.
- **Truncamiento**: textos de más de 4.900 bytes se cortan (`ANALYZED_TRUNCATED`); con resúmenes de
  RSS casi no ocurre.
- **Idiomas**: solo `es` y `en` están contemplados; otros quedan `SKIPPED_UNSUPPORTED_LANGUAGE`.
- **Frases clave genéricas**: Comprehend devuelve sintagmas nominales ("el país", "este año") que
  aparecen en muchas noticias y dominan los rankings; no se filtró una lista de palabras vacías.
- **Cambios del modelo**: si AWS actualiza Comprehend, el mismo texto podría dar otro resultado, pero
  la caché conserva el primero. Para forzar un nuevo análisis se cambia `ANALYSIS_VERSION` en
  `comprehend_enricher.py`.
