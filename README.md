# Spring Boot Reactive (WebFlux + Project Reactor)

Curso práctico para desarrollar APIs no bloqueantes con Spring WebFlux y Project Reactor, optimizadas para alta concurrencia y orientadas a entornos empresariales.

## Estructura

- `CapituloXX/README.md`: guía de laboratorio por capítulo.

## Lista de laboratorios

### Capítulo 1

- [Primeros pipelines reactivos con operadores](Capitulo01/README.md#primeros-pipelines-reactivos-con-operadores)
  - Descripción: Desarrollar primeros pipelines reactivos con operadores de Project Reactor, aplicando Mono, Flux, Publisher, Subscriber, backpressure y control de demanda para diferenciar escenarios bloqueantes y no bloqueantes.
  - Duración estimada: 90 min

### Capítulo 2

- [Realiza una práctica referente a las 4 lecciones de este capítulo](Capitulo02/README.md#realiza-una-práctica-referente-a-las-4-lecciones-de-este-capítulo)
  - Descripción: Construir una API reactiva con Spring WebFlux aplicando controllers reactivos, endpoints funcionales, validación, manejo de errores, filtros y consumo no bloqueante con WebClient.
  - Duración estimada: 80 min

### Capítulo 3

- [Integrar BD reactiva + endpoints de consulta](Capitulo03/README.md#integrar-bd-reactiva-endpoints-de-consulta)
  - Descripción: Integrar persistencia reactiva y endpoints de consulta mediante R2DBC o Mongo Reactive, implementando repositorios reactivos, acceso a datos no bloqueante, transacciones, consistencia, integración externa, paginación y stream de datos.
  - Duración estimada: 155 min

### Capítulo 4

- [Tests + métricas + troubleshooting](Capitulo04/README.md#tests-métricas-troubleshooting)
  - Descripción: Implementar pruebas, métricas y troubleshooting en una API reactiva usando StepVerifier, WebTestClient, Actuator, health checks y buenas prácticas para evitar bloqueo accidental.
  - Duración estimada: 100 min

### Capítulo 5

- [Proyecto Demo end-to-end con pruebas y observabilidad](Capitulo05/README.md#proyecto-demo-end-to-end-con-pruebas-y-observabilidad)
  - Descripción: Construir un proyecto demo end-to-end de una API reactiva empresarial, integrando diseño de entidades, endpoints, validaciones, persistencia no bloqueante, manejo de errores, retries, tests, métricas y checklist de calidad.
  - Duración estimada: 30 min

## Flujo de colaboración

- Trabajar en `changes_course`.
- Crear Pull Request hacia `main`.
- Merge por `Squash and merge`.
