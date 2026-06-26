# Spring Boot con WebFlux — Reactive Store

Curso práctico para desarrollar APIs no bloqueantes con Spring WebFlux y Project Reactor.

## Contrato técnico común

Todos los capítulos trabajan sobre el mismo proyecto:

| Elemento | Valor |
|---|---|
| Proyecto y carpeta | `reactive-store` |
| Paquete base | `com.netec.reactivestore` |
| Entidad principal | `Product` |
| Endpoint base | `/api/products` |
| Puerto | `${SERVER_PORT:8080}` |
| Base URL | `${API_BASE_URL:http://localhost:8080}` |
| Cliente HTTP | `WebClient` |
| Java | 21 LTS |
| Spring Boot | 3.3.5 |
| Build tool | Maven |

Modelo canónico `Product`:

- `id`
- `name`
- `description`
- `price`
- `stock`
- `active`

`Category` puede utilizarse cuando el laboratorio de persistencia necesita clasificación, sin sustituir a `Product` como recurso principal.

Clases canónicas:

- `ProductController`
- `ProductHandler`
- `ProductRouter`
- `ProductService`
- `ProductRepository`
- `ProductDTO`
- `ProductRequest`
- `ProductResponse`
- `ApiResponse`
- `GlobalExceptionHandler`

Placeholders estándar:

- `<PROJECT_NAME>` → `reactive-store`
- `<PACKAGE_NAME>` → `com.netec.reactivestore`
- `<SERVER_PORT>` → `8080`
- `<DATABASE_URL>`
- `<DATABASE_USERNAME>`
- `<DATABASE_PASSWORD>`
- `<API_BASE_URL>` → `http://localhost:8080`
- `<EXTERNAL_API_URL>`
- `<JWT_SECRET>`
- `<PROFILE_NAME>`

Los ejemplos de configuración usan variables de entorno equivalentes, por ejemplo `${DATABASE_URL}`.

## Laboratorios

### Capítulo 1

[Primeros pipelines reactivos con operadores](Capitulo01/README.md)

- Mono, Flux, operadores, backpressure y manejo reactivo de errores.
- Crea la base `reactive-store` y el modelo `Product`.
- Duración estimada: 90 min.

### Capítulo 2

[API de productos con controllers y endpoints funcionales](Capitulo02/README.md)

- Controllers reactivos, RouterFunction, HandlerFunction, validación, errores, SSE y WebClient.
- Mantiene el repositorio en memoria.
- Duración estimada: 80 min.

### Capítulo 3

[Persistencia reactiva y endpoints de consulta](Capitulo03/README.md)

- R2DBC o Mongo Reactive, repositorios, transacciones, paginación, streams e integración externa.
- Sustituye la persistencia en memoria sin cambiar el contrato HTTP.
- Duración estimada: 155 min.

### Capítulo 4

[Tests, métricas y troubleshooting](Capitulo04/README.md)

- StepVerifier, WebTestClient, Actuator, Micrometer y antipatrones reactivos.
- Duración estimada: 100 min.

### Capítulo 5

[Proyecto integrador Reactive Store](Capitulo05/README.md)

- Integración y validación final del mismo proyecto.
- Duración estimada: 30 min.

## Reglas de continuidad

- No crear un proyecto nuevo entre capítulos.
- No cambiar el paquete base.
- No sustituir `Product` por dominios de tareas o pedidos.
- Mantener `/api/products` como endpoint público.
- No mezclar Maven y Gradle.
- No usar `block()` ni suscripciones manuales en controllers o servicios.
- Configurar credenciales y URLs externas mediante variables de entorno.

## Flujo de colaboración

- Trabajar en la rama `changes_course`.
- Crear Pull Request hacia `main`.
- Usar `Squash and merge`.

## Seguridad y configuración local

1. Copia `.env.example` como `.env` solo para desarrollo.
2. Cambia las contraseñas de ejemplo antes de iniciar servicios.
3. No publiques `.env`, tokens, contraseñas ni `JWT_SECRET`.
4. No registres cuerpos, cookies ni cabeceras de autorización.
5. Los endpoints del laboratorio no deben exponerse directamente a Internet.

`.gitignore` excluye secretos locales, logs, artefactos Maven y archivos comunes del IDE.

## Convenciones multiplataforma

- Bash/Zsh: usa los bloques `bash`.
- PowerShell: usa `Invoke-RestMethod` para JSON.
- Variables: `$NOMBRE` en Bash/Zsh y `$env:NOMBRE` en PowerShell.
- Detén Spring Boot con `Ctrl+C` en la terminal que lo inició.
- Limita la limpieza a `docker compose down`; no elimines recursos globales.
