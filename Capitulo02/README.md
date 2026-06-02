# Task Management API — Construyendo una API Reactiva Completa con Spring WebFlux

## 1. Metadatos

| Campo            | Valor                                      |
|------------------|--------------------------------------------|
| **Duración**     | 80 minutos                                 |
| **Complejidad**  | Media                                      |
| **Nivel Bloom**  | Aplicar (Apply)                            |
| **Módulo**       | 2 — Spring WebFlux: APIs REST No Bloqueantes |
| **Lab anterior** | Lab 01-00-01 (Mono, Flux y operadores base) |

---

## 2. Descripción General

En esta práctica construirás una **Task Management API** completamente no bloqueante usando Spring WebFlux. Implementarás los mismos recursos CRUD con dos estilos arquitectónicos distintos —modelo de anotaciones y functional endpoints— para comparar sus diferencias en un contexto real. Añadirás soporte para streaming via Server-Sent Events (SSE), validación de entrada con respuestas estructuradas RFC 7807, y consumo de una API externa con WebClient configurado con timeouts y fallback reactivo. Todo el estado se mantiene en memoria (`ConcurrentHashMap`) para que la práctica sea autocontenida sin requerir base de datos.

---

## 3. Objetivos de Aprendizaje

- [ ] Implementar los mismos endpoints CRUD usando `@RestController` (modelo de anotaciones) y `RouterFunction + HandlerFunction` (modelo funcional), identificando las diferencias estructurales entre ambos estilos.
- [ ] Configurar negociación de contenido para servir `application/json` y `text/event-stream` (SSE) desde el mismo recurso reactivo.
- [ ] Aplicar validación de entrada con Bean Validation (`@Valid`, `@NotBlank`, `@Size`) en contexto reactivo y devolver errores estructurados como `ProblemDetail` (RFC 7807).
- [ ] Implementar manejo global de errores con `@RestControllerAdvice` y operadores `onErrorResume` / `onErrorMap` en el pipeline reactivo.
- [ ] Consumir una API externa de forma no bloqueante con `WebClient`, configurando timeout de 3 segundos y un fallback reactivo ante fallos.

---

## 4. Prerrequisitos

### Conocimiento previo
- Haber completado **Lab 01-00-01**: comprensión operacional de `Mono<T>` y `Flux<T>`.
- Experiencia con Spring MVC (`@RestController`, `@RequestMapping`, `@GetMapping`, etc.).
- Comprensión del protocolo HTTP: métodos GET/POST/PUT/DELETE, códigos de estado 2xx/4xx/5xx, headers `Content-Type` y `Accept`.
- Nociones básicas de JSON y serialización con Jackson.

### Acceso y herramientas requeridas
- JDK 21 instalado y configurado en `PATH` (`java -version` debe mostrar `21.x`).
- Apache Maven 3.9.x (`mvn -version`).
- IntelliJ IDEA 2024.1+ (Community o Ultimate).
- Conexión a Internet (para descargar dependencias Maven y acceder a `jsonplaceholder.typicode.com`).
- Terminal/shell (PowerShell, bash o zsh).

> ⚠️ **ANTIPATRÓN CRÍTICO**: En toda esta práctica, `block()`, `blockFirst()` y `blockLast()` están **prohibidos** dentro de controllers, handlers o pipelines reactivos. Solo son aceptables en el método `main()` o en tests. Violar esta regla bloquea el event loop de Netty y anula los beneficios reactivos.

---

## 5. Entorno del Laboratorio

### Software requerido

| Componente          | Versión       | Verificación                    |
|---------------------|---------------|---------------------------------|
| JDK                 | 21 LTS        | `java -version`                 |
| Maven               | 3.9.x         | `mvn -version`                  |
| Spring Boot         | 3.3.x         | Ver `pom.xml` generado          |
| Project Reactor     | 3.6.x         | Transitivo via Spring Boot      |
| IntelliJ IDEA       | 2024.1+       | Abrir IDE                       |
| curl / HTTPie       | 8.x / 3.x     | `curl --version`                |

### Configuración inicial del proyecto

Ejecuta los siguientes comandos en tu terminal para generar el proyecto base con Spring Initializr CLI o descárgalo desde [start.spring.io](https://start.spring.io):

```bash
# Opción A: usando curl con Spring Initializr
curl -s "https://start.spring.io/starter.tgz" \
  -d type=maven-project \
  -d language=java \
  -d bootVersion=3.3.5 \
  -d baseDir=task-management-api \
  -d groupId=com.curso.reactivo \
  -d artifactId=task-management-api \
  -d name=task-management-api \
  -d packageName=com.curso.reactivo.tasks \
  -d javaVersion=21 \
  -d dependencies=webflux,validation \
  | tar -xzvf -

cd task-management-api
```

```bash
# Opción B: verificar que el proyecto compila correctamente
mvn clean compile -q
echo "Proyecto generado correctamente: $?"
```

Verifica que el `pom.xml` contiene las dependencias correctas:

```xml
<!-- Dependencias clave que deben estar presentes en pom.xml -->
<dependencies>
    <!-- WebFlux: incluye Netty + Project Reactor -->
    <dependency>
        <groupId>org.springframework.boot</groupId>
        <artifactId>spring-boot-starter-webflux</artifactId>
    </dependency>

    <!-- Bean Validation en contexto reactivo -->
    <dependency>
        <groupId>org.springframework.boot</groupId>
        <artifactId>spring-boot-starter-validation</artifactId>
    </dependency>

    <!-- Testing reactivo -->
    <dependency>
        <groupId>org.springframework.boot</groupId>
        <artifactId>spring-boot-starter-test</artifactId>
        <scope>test</scope>
    </dependency>
    <dependency>
        <groupId>io.projectreactor</groupId>
        <artifactId>reactor-test</artifactId>
        <scope>test</scope>
    </dependency>
</dependencies>
```

Configura `src/main/resources/application.properties`:

```properties
# Configuración base de la aplicación
spring.application.name=task-management-api
server.port=8080

# Logging para ver el event loop de Netty en acción
logging.level.reactor.netty=INFO
logging.level.com.curso.reactivo=DEBUG
```

---

## 6. Pasos del Laboratorio

---

### Bloque 1 — Modelo de Anotaciones vs Functional Endpoints (25 minutos)

#### Paso 1.1 — Crear el modelo de dominio y el repositorio en memoria

**Objetivo:** Definir la entidad `Task` y el repositorio reactivo en memoria que usarán ambos modelos.

**Instrucciones:**

1. Crea el paquete `com.curso.reactivo.tasks.model` y dentro el record `Task`:

```java
// src/main/java/com/curso/reactivo/tasks/model/Task.java
package com.curso.reactivo.tasks.model;

import java.time.LocalDateTime;

/**
 * Entidad de dominio inmutable que representa una tarea.
 * Usamos record de Java 21 para reducir boilerplate.
 */
public record Task(
    String id,
    String title,
    String description,
    TaskStatus status,
    LocalDateTime createdAt
) {
    public enum TaskStatus {
        PENDING, IN_PROGRESS, COMPLETED
    }
}
```

2. Crea el DTO de entrada con validaciones en el paquete `com.curso.reactivo.tasks.dto`:

```java
// src/main/java/com/curso/reactivo/tasks/dto/TaskRequest.java
package com.curso.reactivo.tasks.dto;

import com.curso.reactivo.tasks.model.Task.TaskStatus;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;

/**
 * DTO de entrada para crear o actualizar una tarea.
 * Las anotaciones de Bean Validation se aplican aquí, no en el modelo de dominio.
 */
public record TaskRequest(

    @NotBlank(message = "El título no puede estar vacío")
    @Size(min = 3, max = 100, message = "El título debe tener entre 3 y 100 caracteres")
    String title,

    @Size(max = 500, message = "La descripción no puede superar 500 caracteres")
    String description,

    TaskStatus status
) {}
```

3. Crea el repositorio en memoria en el paquete `com.curso.reactivo.tasks.repository`:

```java
// src/main/java/com/curso/reactivo/tasks/repository/TaskRepository.java
package com.curso.reactivo.tasks.repository;

import com.curso.reactivo.tasks.model.Task;
import com.curso.reactivo.tasks.model.Task.TaskStatus;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.LocalDateTime;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Repositorio reactivo en memoria usando ConcurrentHashMap.
 * Simula operaciones asíncronas devolviendo Mono y Flux.
 * En el Lab 03 esto será reemplazado por R2DBC/MongoDB.
 */
@Repository
public class TaskRepository {

    // ConcurrentHashMap es thread-safe para acceso concurrente
    private final Map<String, Task> store = new ConcurrentHashMap<>();

    public TaskRepository() {
        // Datos iniciales para facilitar las pruebas
        Task t1 = new Task(UUID.randomUUID().toString(), "Configurar proyecto WebFlux",
                "Crear estructura base con Spring Initializr", TaskStatus.COMPLETED,
                LocalDateTime.now().minusDays(2));
        Task t2 = new Task(UUID.randomUUID().toString(), "Implementar endpoints reactivos",
                "Usar Mono y Flux en los controllers", TaskStatus.IN_PROGRESS,
                LocalDateTime.now().minusHours(3));
        Task t3 = new Task(UUID.randomUUID().toString(), "Escribir tests con StepVerifier",
                "Verificar el comportamiento reactivo", TaskStatus.PENDING,
                LocalDateTime.now());
        store.put(t1.id(), t1);
        store.put(t2.id(), t2);
        store.put(t3.id(), t3);
    }

    public Flux<Task> findAll() {
        // Flux.fromIterable envuelve una colección síncrona en un Flux reactivo
        return Flux.fromIterable(store.values());
    }

    public Mono<Task> findById(String id) {
        // Mono.justOrEmpty maneja correctamente el caso null (devuelve empty Mono)
        return Mono.justOrEmpty(store.get(id));
    }

    public Mono<Task> save(TaskRequest request) {
        // Aquí importaría el DTO — ajusta el import según tu estructura
        String id = UUID.randomUUID().toString();
        Task task = new Task(id, request.title(), request.description(),
                request.status() != null ? request.status() : TaskStatus.PENDING,
                LocalDateTime.now());
        store.put(id, task);
        return Mono.just(task);
    }

    public Mono<Task> update(String id, TaskRequest request) {
        return findById(id)
            .map(existing -> {
                Task updated = new Task(
                    existing.id(),
                    request.title() != null ? request.title() : existing.title(),
                    request.description() != null ? request.description() : existing.description(),
                    request.status() != null ? request.status() : existing.status(),
                    existing.createdAt()
                );
                store.put(id, updated);
                return updated;
            });
    }

    public Mono<Void> deleteById(String id) {
        store.remove(id);
        return Mono.empty();
    }

    // Método auxiliar para el endpoint de streaming
    public Flux<Task> findAll(int delaySeconds) {
        return findAll().delayElements(java.time.Duration.ofSeconds(delaySeconds));
    }
}
```

> **Nota:** Agrega el import de `TaskRequest` al repositorio: `import com.curso.reactivo.tasks.dto.TaskRequest;`

**Salida esperada:** El proyecto compila sin errores (`mvn compile -q` retorna código 0).

**Verificación:**
```bash
mvn compile -q && echo "✅ Compilación exitosa"
```

---

#### Paso 1.2 — Implementar el controlador con modelo de anotaciones

**Objetivo:** Crear un `@RestController` reactivo completo con los endpoints CRUD de tareas.

**Instrucciones:**

1. Crea la clase `TaskController` en el paquete `com.curso.reactivo.tasks.controller`:

```java
// src/main/java/com/curso/reactivo/tasks/controller/TaskController.java
package com.curso.reactivo.tasks.controller;

import com.curso.reactivo.tasks.dto.TaskRequest;
import com.curso.reactivo.tasks.model.Task;
import com.curso.reactivo.tasks.repository.TaskRepository;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;

/**
 * Controlador reactivo usando el modelo de anotaciones.
 * Observa que los métodos retornan Mono/Flux, no objetos directos.
 * Esto garantiza que el procesamiento sea completamente no bloqueante.
 */
@RestController
@RequestMapping("/api/tasks")
public class TaskController {

    private final TaskRepository repository;

    public TaskController(TaskRepository repository) {
        this.repository = repository;
    }

    /**
     * GET /api/tasks — Lista todas las tareas.
     * Retorna Flux<Task>: puede emitir 0 o N elementos.
     */
    @GetMapping
    public Flux<Task> getAllTasks() {
        return repository.findAll();
    }

    /**
     * GET /api/tasks/{id} — Obtiene una tarea por ID.
     * Retorna Mono<ResponseEntity<Task>>: permite controlar el código HTTP.
     * Si no existe, retorna 404 usando switchIfEmpty.
     */
    @GetMapping("/{id}")
    public Mono<ResponseEntity<Task>> getTaskById(@PathVariable String id) {
        return repository.findById(id)
            .map(task -> ResponseEntity.ok(task))
            // switchIfEmpty se activa cuando el Mono upstream está vacío (tarea no encontrada)
            .switchIfEmpty(Mono.just(ResponseEntity.notFound().<Task>build()));
    }

    /**
     * POST /api/tasks — Crea una nueva tarea.
     * @Valid activa Bean Validation; si falla, WebFlux lanza WebExchangeBindException.
     * El @RestControllerAdvice (Paso 3.1) interceptará esa excepción.
     */
    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public Mono<Task> createTask(@Valid @RequestBody TaskRequest request) {
        return repository.save(request);
    }

    /**
     * PUT /api/tasks/{id} — Actualiza una tarea existente.
     * onErrorResume captura el caso de tarea no encontrada y retorna 404.
     */
    @PutMapping("/{id}")
    public Mono<ResponseEntity<Task>> updateTask(
            @PathVariable String id,
            @Valid @RequestBody TaskRequest request) {
        return repository.update(id, request)
            .map(task -> ResponseEntity.ok(task))
            .switchIfEmpty(Mono.just(ResponseEntity.notFound().<Task>build()));
    }

    /**
     * DELETE /api/tasks/{id} — Elimina una tarea.
     * Retorna 204 No Content.
     */
    @DeleteMapping("/{id}")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public Mono<Void> deleteTask(@PathVariable String id) {
        return repository.deleteById(id);
    }

    /**
     * GET /api/tasks/stream — Streaming SSE de tareas.
     * BLOQUE 2: Se implementará en el Paso 2.1.
     * Aquí se incluye como placeholder para que el router funcional no colisione.
     */
    @GetMapping(value = "/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<Task> streamTasks() {
        // Emite todas las tareas con un delay de 1 segundo entre cada una
        return repository.findAll()
            .delayElements(Duration.ofSeconds(1));
    }
}
```

2. Inicia la aplicación y verifica que arranca correctamente:

```bash
mvn spring-boot:run
```

**Salida esperada:**
```
  .   ____          _            __ _ _
 /\\ / ___'_ __ _ _(_)_ __  __ _ \ \ \ \
...
Started TaskManagementApiApplication in X.XXX seconds (process running for X.XXX)
```

**Verificación:**
```bash
# Listar todas las tareas
curl -s http://localhost:8080/api/tasks | python3 -m json.tool

# Crear una tarea nueva
curl -s -X POST http://localhost:8080/api/tasks \
  -H "Content-Type: application/json" \
  -d '{"title":"Mi primera tarea reactiva","description":"Creada desde curl","status":"PENDING"}' \
  | python3 -m json.tool
```

Debes ver un array JSON con las 3 tareas iniciales y luego el objeto de la tarea creada con `201 Created`.

---

#### Paso 1.3 — Implementar el mismo CRUD con Functional Endpoints

**Objetivo:** Replicar la funcionalidad del controlador anterior usando `RouterFunction` y `HandlerFunction`, exponiendo los endpoints bajo `/functional/tasks` para que ambos coexistan.

**Instrucciones:**

1. Crea el `TaskHandler` en el paquete `com.curso.reactivo.tasks.functional`:

```java
// src/main/java/com/curso/reactivo/tasks/functional/TaskHandler.java
package com.curso.reactivo.tasks.functional;

import com.curso.reactivo.tasks.dto.TaskRequest;
import com.curso.reactivo.tasks.model.Task;
import com.curso.reactivo.tasks.repository.TaskRepository;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.server.ServerRequest;
import org.springframework.web.reactive.function.server.ServerResponse;
import reactor.core.publisher.Mono;

/**
 * Handler funcional: recibe ServerRequest y retorna Mono<ServerResponse>.
 * No tiene anotaciones de mapeo — las rutas se definen en TaskRouter.
 * Esto lo hace más fácil de testear unitariamente (función pura).
 */
@Component
public class TaskHandler {

    private final TaskRepository repository;

    public TaskHandler(TaskRepository repository) {
        this.repository = repository;
    }

    /**
     * GET /functional/tasks
     * En el modelo funcional, el tipo de contenido se especifica explícitamente.
     */
    public Mono<ServerResponse> getAllTasks(ServerRequest request) {
        return ServerResponse.ok()
            .contentType(MediaType.APPLICATION_JSON)
            .body(repository.findAll(), Task.class);
    }

    /**
     * GET /functional/tasks/{id}
     * switchIfEmpty maneja el caso "no encontrado" de forma declarativa.
     * Más explícito que lanzar una excepción.
     */
    public Mono<ServerResponse> getTaskById(ServerRequest request) {
        String id = request.pathVariable("id");
        return repository.findById(id)
            .flatMap(task -> ServerResponse.ok()
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(task))
            .switchIfEmpty(ServerResponse.notFound().build());
    }

    /**
     * POST /functional/tasks
     * flatMap encadena la extracción del body con la operación de guardado.
     */
    public Mono<ServerResponse> createTask(ServerRequest request) {
        return request.bodyToMono(TaskRequest.class)
            .flatMap(taskRequest -> repository.save(taskRequest))
            .flatMap(savedTask -> ServerResponse
                .status(HttpStatus.CREATED)
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(savedTask));
    }

    /**
     * PUT /functional/tasks/{id}
     */
    public Mono<ServerResponse> updateTask(ServerRequest request) {
        String id = request.pathVariable("id");
        return request.bodyToMono(TaskRequest.class)
            .flatMap(taskRequest -> repository.update(id, taskRequest))
            .flatMap(updatedTask -> ServerResponse.ok()
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(updatedTask))
            .switchIfEmpty(ServerResponse.notFound().build());
    }

    /**
     * DELETE /functional/tasks/{id}
     */
    public Mono<ServerResponse> deleteTask(ServerRequest request) {
        String id = request.pathVariable("id");
        return repository.deleteById(id)
            .then(ServerResponse.noContent().build());
    }
}
```

2. Crea el `TaskRouter` que define las rutas:

```java
// src/main/java/com/curso/reactivo/tasks/functional/TaskRouter.java
package com.curso.reactivo.tasks.functional;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.reactive.function.server.RouterFunction;
import org.springframework.web.reactive.function.server.RouterFunctions;
import org.springframework.web.reactive.function.server.ServerResponse;

import static org.springframework.web.reactive.function.server.RequestPredicates.*;

/**
 * Configuración de rutas funcionales.
 * Todas las rutas se definen explícitamente aquí — no hay "magia" de anotaciones.
 * Esto facilita la auditoría de rutas y la composición de routers.
 */
@Configuration
public class TaskRouter {

    @Bean
    public RouterFunction<ServerResponse> taskRoutes(TaskHandler handler) {
        return RouterFunctions
            // Ruta base: /functional/tasks
            .route(GET("/functional/tasks"), handler::getAllTasks)
            .andRoute(POST("/functional/tasks"), handler::createTask)
            // Rutas con path variable: /functional/tasks/{id}
            .andRoute(GET("/functional/tasks/{id}"), handler::getTaskById)
            .andRoute(PUT("/functional/tasks/{id}"), handler::updateTask)
            .andRoute(DELETE("/functional/tasks/{id}"), handler::deleteTask);
    }
}
```

**Salida esperada:** Ambos sets de endpoints responden correctamente.

**Verificación:**
```bash
# Probar el endpoint funcional
curl -s http://localhost:8080/functional/tasks | python3 -m json.tool

# Comparar: ambas rutas deben retornar los mismos datos
echo "=== Anotaciones ===" && curl -s http://localhost:8080/api/tasks
echo "=== Funcional ===" && curl -s http://localhost:8080/functional/tasks
```

> 💡 **Reflexión:** Observa que ambos endpoints usan el mismo `TaskRepository`. La diferencia es puramente arquitectónica: cómo se mapean las rutas y cómo se expresa la lógica. El modelo funcional es más verboso pero más explícito sobre el flujo de datos.

---

### Bloque 2 — Negociación de Contenido y Server-Sent Events (15 minutos)

#### Paso 2.1 — Configurar el endpoint de streaming SSE

**Objetivo:** Crear un endpoint que emita tareas como Server-Sent Events, demostrando la negociación de contenido en WebFlux.

**Instrucciones:**

1. El endpoint SSE ya está declarado en `TaskController` (Paso 1.2). Ahora añade un endpoint de streaming infinito que simule tareas llegando en tiempo real. Agrega este método adicional al `TaskController`:

```java
// Añadir dentro de TaskController.java

/**
 * GET /api/tasks/live-stream — Stream infinito de tareas (simulación tiempo real).
 * Produce text/event-stream: el cliente mantiene la conexión abierta.
 * Interval genera un tick cada 2 segundos; flatMap consulta el repositorio en cada tick.
 *
 * IMPORTANTE: Este endpoint NUNCA cierra la conexión por sí solo.
 * El cliente debe desconectarse o usar take() para limitar los elementos.
 */
@GetMapping(value = "/live-stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
public Flux<Task> liveStreamTasks() {
    return Flux.interval(Duration.ofSeconds(2))
        // En cada tick, emite todas las tareas actuales
        .flatMap(tick -> repository.findAll())
        // take(10) limita a 10 emisiones para no saturar en el lab
        .take(10)
        .doOnNext(task -> log.debug("Emitiendo tarea via SSE: {}", task.id()));
}
```

2. Añade el import y el logger al controlador:

```java
// Imports adicionales necesarios en TaskController.java
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// Dentro de la clase TaskController, antes del constructor:
private static final Logger log = LoggerFactory.getLogger(TaskController.class);
```

3. Añade también el endpoint SSE al router funcional. Modifica `TaskRouter.java`:

```java
// Modificar TaskRouter.java — añadir la ruta SSE funcional
@Bean
public RouterFunction<ServerResponse> taskRoutes(TaskHandler handler) {
    return RouterFunctions
        .route(GET("/functional/tasks"), handler::getAllTasks)
        .andRoute(POST("/functional/tasks"), handler::createTask)
        .andRoute(GET("/functional/tasks/{id}"), handler::getTaskById)
        .andRoute(PUT("/functional/tasks/{id}"), handler::updateTask)
        .andRoute(DELETE("/functional/tasks/{id}"), handler::deleteTask)
        // Nuevo: endpoint SSE funcional
        .andRoute(GET("/functional/tasks/stream"), handler::streamTasks);
}
```

4. Añade el método `streamTasks` al `TaskHandler`:

```java
// Añadir en TaskHandler.java

/**
 * GET /functional/tasks/stream — SSE via modelo funcional.
 * ServerResponse.ok().contentType(TEXT_EVENT_STREAM) activa el modo streaming.
 */
public Mono<ServerResponse> streamTasks(ServerRequest request) {
    return ServerResponse.ok()
        .contentType(MediaType.TEXT_EVENT_STREAM)
        .body(repository.findAll().delayElements(Duration.ofSeconds(1)), Task.class);
}
```

> ⚠️ **Orden de rutas en el modelo funcional:** La ruta `/functional/tasks/stream` debe registrarse **antes** de `/functional/tasks/{id}` en el router para evitar que `stream` sea interpretado como un `{id}`. Verifica el orden en `TaskRouter`.

**Verificación:**
```bash
# Probar SSE — observa cómo llegan los eventos uno por uno
curl -N -H "Accept: text/event-stream" http://localhost:8080/api/tasks/stream

# Probar SSE funcional
curl -N -H "Accept: text/event-stream" http://localhost:8080/functional/tasks/stream
```

Debes ver la salida en formato SSE:
```
data:{"id":"...","title":"Configurar proyecto WebFlux",...}

data:{"id":"...","title":"Implementar endpoints reactivos",...}
```

---

### Bloque 3 — Validación y Manejo Global de Errores (20 minutos)

#### Paso 3.1 — Implementar el manejador global de errores con ProblemDetail

**Objetivo:** Crear un `@RestControllerAdvice` que intercepte errores de validación y otros errores del dominio, devolviendo respuestas estructuradas según RFC 7807 (`ProblemDetail`).

**Instrucciones:**

1. Crea la excepción de dominio `TaskNotFoundException`:

```java
// src/main/java/com/curso/reactivo/tasks/exception/TaskNotFoundException.java
package com.curso.reactivo.tasks.exception;

/**
 * Excepción de dominio para tareas no encontradas.
 * Extiende RuntimeException para ser compatible con el pipeline reactivo.
 */
public class TaskNotFoundException extends RuntimeException {

    private final String taskId;

    public TaskNotFoundException(String taskId) {
        super("Tarea no encontrada con ID: " + taskId);
        this.taskId = taskId;
    }

    public String getTaskId() {
        return taskId;
    }
}
```

2. Crea el manejador global de errores:

```java
// src/main/java/com/curso/reactivo/tasks/exception/GlobalErrorHandler.java
package com.curso.reactivo.tasks.exception;

import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.validation.FieldError;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.bind.support.WebExchangeBindException;
import reactor.core.publisher.Mono;

import java.net.URI;
import java.time.Instant;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Manejador global de errores reactivo.
 *
 * @RestControllerAdvice intercepta excepciones de todos los @RestController.
 * Los métodos retornan Mono<ProblemDetail> para mantener el pipeline reactivo.
 *
 * ProblemDetail implementa RFC 7807 — estándar para respuestas de error HTTP.
 * Spring 6+ incluye soporte nativo para ProblemDetail.
 */
@RestControllerAdvice
public class GlobalErrorHandler {

    /**
     * Maneja errores de validación (@Valid falló).
     * WebExchangeBindException es la excepción reactiva equivalente a
     * MethodArgumentNotValidException de Spring MVC.
     */
    @ExceptionHandler(WebExchangeBindException.class)
    public Mono<ProblemDetail> handleValidationErrors(WebExchangeBindException ex) {

        // Extraer todos los errores de campo como un mapa campo -> mensaje
        Map<String, String> fieldErrors = ex.getBindingResult()
            .getFieldErrors()
            .stream()
            .collect(Collectors.toMap(
                FieldError::getField,
                fe -> fe.getDefaultMessage() != null ? fe.getDefaultMessage() : "Error de validación",
                // En caso de múltiples errores para el mismo campo, tomar el primero
                (existing, replacement) -> existing
            ));

        // Crear ProblemDetail con status 400
        ProblemDetail problem = ProblemDetail.forStatus(HttpStatus.BAD_REQUEST);
        problem.setType(URI.create("https://api.task-management.com/errors/validation"));
        problem.setTitle("Error de validación");
        problem.setDetail("La solicitud contiene campos inválidos");
        // Añadir propiedad personalizada con los errores de campo
        problem.setProperty("fieldErrors", fieldErrors);
        problem.setProperty("timestamp", Instant.now().toString());

        return Mono.just(problem);
    }

    /**
     * Maneja el caso de tarea no encontrada.
     * Retorna 404 con ProblemDetail estructurado.
     */
    @ExceptionHandler(TaskNotFoundException.class)
    public Mono<ProblemDetail> handleTaskNotFound(TaskNotFoundException ex) {
        ProblemDetail problem = ProblemDetail.forStatus(HttpStatus.NOT_FOUND);
        problem.setType(URI.create("https://api.task-management.com/errors/not-found"));
        problem.setTitle("Recurso no encontrado");
        problem.setDetail(ex.getMessage());
        problem.setProperty("taskId", ex.getTaskId());
        problem.setProperty("timestamp", Instant.now().toString());

        return Mono.just(problem);
    }

    /**
     * Manejador genérico para excepciones no previstas.
     * Retorna 500 sin exponer detalles internos al cliente.
     */
    @ExceptionHandler(Exception.class)
    public Mono<ProblemDetail> handleGenericError(Exception ex) {
        ProblemDetail problem = ProblemDetail.forStatus(HttpStatus.INTERNAL_SERVER_ERROR);
        problem.setType(URI.create("https://api.task-management.com/errors/internal"));
        problem.setTitle("Error interno del servidor");
        problem.setDetail("Se produjo un error inesperado. Contacte al administrador.");
        problem.setProperty("timestamp", Instant.now().toString());

        return Mono.just(problem);
    }
}
```

3. Actualiza el método `getTaskById` en `TaskController` para usar la excepción de dominio con `onErrorResume`:

```java
// Reemplazar el método getTaskById en TaskController.java

@GetMapping("/{id}")
public Mono<ResponseEntity<Task>> getTaskById(@PathVariable String id) {
    return repository.findById(id)
        .map(task -> ResponseEntity.ok(task))
        // switchIfEmpty lanza la excepción de dominio cuando no se encuentra la tarea
        // El GlobalErrorHandler la interceptará y retornará ProblemDetail con 404
        .switchIfEmpty(Mono.error(new TaskNotFoundException(id)));
}
```

**Verificación:**
```bash
# Probar validación — título vacío debe retornar 400 con ProblemDetail
curl -s -X POST http://localhost:8080/api/tasks \
  -H "Content-Type: application/json" \
  -d '{"title":"","description":"Sin título"}' \
  | python3 -m json.tool

# Probar tarea no encontrada — debe retornar 404 con ProblemDetail
curl -s http://localhost:8080/api/tasks/id-inexistente \
  | python3 -m json.tool

# Probar título demasiado corto (menos de 3 caracteres)
curl -s -X POST http://localhost:8080/api/tasks \
  -H "Content-Type: application/json" \
  -d '{"title":"AB","description":"Título muy corto"}' \
  | python3 -m json.tool
```

**Salida esperada para validación fallida:**
```json
{
  "type": "https://api.task-management.com/errors/validation",
  "title": "Error de validación",
  "status": 400,
  "detail": "La solicitud contiene campos inválidos",
  "fieldErrors": {
    "title": "El título debe tener entre 3 y 100 caracteres"
  },
  "timestamp": "2024-..."
}
```

---

#### Paso 3.2 — Aplicar onErrorResume y onErrorMap en el pipeline

**Objetivo:** Demostrar el uso de operadores de manejo de errores directamente en el pipeline reactivo, sin depender exclusivamente del `@RestControllerAdvice`.

**Instrucciones:**

1. Añade un endpoint de actualización con manejo de errores en el pipeline al `TaskController`:

```java
// Añadir en TaskController.java — reemplazar el método updateTask existente

@PutMapping("/{id}")
public Mono<ResponseEntity<Task>> updateTask(
        @PathVariable String id,
        @Valid @RequestBody TaskRequest request) {
    return repository.update(id, request)
        .map(task -> ResponseEntity.ok(task))
        // onErrorResume: si ocurre cualquier excepción en el pipeline,
        // transformarla en una respuesta 404 estructurada
        .onErrorResume(TaskNotFoundException.class, ex ->
            Mono.just(ResponseEntity.notFound().<Task>build()))
        // switchIfEmpty: si el Mono está vacío (tarea no encontrada sin excepción)
        .switchIfEmpty(Mono.error(new TaskNotFoundException(id)));
}
```

2. Añade un método que demuestre `onErrorMap` (transformación de errores):

```java
// Añadir en TaskController.java — endpoint de demostración de onErrorMap

/**
 * GET /api/tasks/{id}/safe — Versión con manejo explícito de errores en el pipeline.
 * Demuestra onErrorMap: transforma un tipo de excepción en otro.
 */
@GetMapping("/{id}/safe")
public Mono<Task> getTaskByIdSafe(@PathVariable String id) {
    return repository.findById(id)
        // switchIfEmpty lanza TaskNotFoundException si el Mono está vacío
        .switchIfEmpty(Mono.error(new TaskNotFoundException(id)))
        // onErrorMap transforma TaskNotFoundException en RuntimeException genérica
        // Útil para ocultar detalles de implementación al cliente
        .onErrorMap(TaskNotFoundException.class,
            ex -> new RuntimeException("Error al recuperar la tarea: " + ex.getMessage()));
}
```

**Verificación:**
```bash
# Verificar que onErrorMap transforma la excepción correctamente
curl -v http://localhost:8080/api/tasks/no-existe/safe 2>&1 | grep -E "HTTP|title|detail"
```

---

### Bloque 4 — WebClient: Consumo No Bloqueante de API Externa (20 minutos)

#### Paso 4.1 — Configurar WebClient con timeout y manejo de errores

**Objetivo:** Crear un `WebClient` configurado con timeout de 3 segundos que consuma la API pública JSONPlaceholder para enriquecer las tareas con datos de usuario.

**Instrucciones:**

1. Crea el DTO para la respuesta de JSONPlaceholder en `com.curso.reactivo.tasks.dto`:

```java
// src/main/java/com/curso/reactivo/tasks/dto/JsonPlaceholderUser.java
package com.curso.reactivo.tasks.dto;

/**
 * DTO para deserializar la respuesta de https://jsonplaceholder.typicode.com/users/{id}
 */
public record JsonPlaceholderUser(
    Integer id,
    String name,
    String email,
    String phone,
    String website
) {}
```

2. Crea el DTO de respuesta enriquecida:

```java
// src/main/java/com/curso/reactivo/tasks/dto/EnrichedTask.java
package com.curso.reactivo.tasks.dto;

import com.curso.reactivo.tasks.model.Task;

/**
 * Tarea enriquecida con datos del usuario asignado.
 * Combina datos locales (Task) con datos externos (JsonPlaceholderUser).
 */
public record EnrichedTask(
    Task task,
    JsonPlaceholderUser assignedUser,
    String enrichmentSource
) {}
```

3. Crea la configuración de `WebClient` en `com.curso.reactivo.tasks.config`:

```java
// src/main/java/com/curso/reactivo/tasks/config/WebClientConfig.java
package com.curso.reactivo.tasks.config;

import io.netty.channel.ChannelOption;
import io.netty.handler.timeout.ReadTimeoutHandler;
import io.netty.handler.timeout.WriteTimeoutHandler;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.netty.http.client.HttpClient;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * Configuración centralizada de WebClient.
 *
 * Se configura con:
 * - Timeout de conexión: 3 segundos
 * - Timeout de lectura: 3 segundos
 * - Timeout de escritura: 3 segundos
 *
 * Esto garantiza que el pipeline reactivo no quede bloqueado
 * esperando indefinidamente una respuesta externa.
 */
@Configuration
public class WebClientConfig {

    @Bean
    public WebClient jsonPlaceholderClient() {
        // Configurar el cliente HTTP de Netty con timeouts
        HttpClient httpClient = HttpClient.create()
            // Timeout para establecer la conexión TCP
            .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 3000)
            // Timeout para operaciones de respuesta
            .responseTimeout(Duration.ofSeconds(3))
            .doOnConnected(conn -> conn
                // Timeout para leer datos del servidor
                .addHandlerLast(new ReadTimeoutHandler(3, TimeUnit.SECONDS))
                // Timeout para escribir datos al servidor
                .addHandlerLast(new WriteTimeoutHandler(3, TimeUnit.SECONDS)));

        return WebClient.builder()
            .baseUrl("https://jsonplaceholder.typicode.com")
            .clientConnector(new ReactorClientHttpConnector(httpClient))
            // Header por defecto para todas las solicitudes
            .defaultHeader("Accept", "application/json")
            .defaultHeader("User-Agent", "TaskManagementAPI/1.0")
            .build();
    }
}
```

4. Crea el servicio de enriquecimiento en `com.curso.reactivo.tasks.service`:

```java
// src/main/java/com/curso/reactivo/tasks/service/TaskEnrichmentService.java
package com.curso.reactivo.tasks.service;

import com.curso.reactivo.tasks.dto.EnrichedTask;
import com.curso.reactivo.tasks.dto.JsonPlaceholderUser;
import com.curso.reactivo.tasks.model.Task;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import java.time.Duration;

/**
 * Servicio que enriquece tareas con datos de usuario de JSONPlaceholder.
 *
 * Demuestra:
 * - Consumo no bloqueante con WebClient
 * - Manejo de errores HTTP (4xx, 5xx)
 * - Fallback reactivo con onErrorResume
 * - Reintentos automáticos con retry()
 */
@Service
public class TaskEnrichmentService {

    private static final Logger log = LoggerFactory.getLogger(TaskEnrichmentService.class);

    private final WebClient webClient;

    public TaskEnrichmentService(WebClient jsonPlaceholderClient) {
        this.webClient = jsonPlaceholderClient;
    }

    /**
     * Enriquece una tarea con datos del usuario de JSONPlaceholder.
     * Usa el índice de la tarea (1-10) para mapear al userId de JSONPlaceholder.
     *
     * @param task La tarea a enriquecer
     * @param userId ID del usuario en JSONPlaceholder (1-10)
     * @return Mono<EnrichedTask> con los datos combinados
     */
    public Mono<EnrichedTask> enrichTask(Task task, int userId) {
        return webClient.get()
            .uri("/users/{id}", userId)
            .retrieve()
            // onStatus intercepta respuestas con códigos de error HTTP
            .onStatus(
                status -> status.is4xxClientError(),
                response -> Mono.error(new RuntimeException(
                    "Usuario no encontrado en JSONPlaceholder: " + userId))
            )
            .onStatus(
                status -> status.is5xxServerError(),
                response -> Mono.error(new RuntimeException(
                    "Error del servidor externo: " + response.statusCode()))
            )
            .bodyToMono(JsonPlaceholderUser.class)
            // Reintentar hasta 2 veces con backoff exponencial si hay error de red
            .retryWhen(Retry.backoff(2, Duration.ofMillis(500))
                .filter(ex -> !(ex instanceof WebClientResponseException))
                .doBeforeRetry(signal ->
                    log.warn("Reintentando solicitud a JSONPlaceholder. Intento: {}",
                        signal.totalRetries() + 1)))
            // Combinar la tarea con los datos del usuario
            .map(user -> new EnrichedTask(task, user, "jsonplaceholder.typicode.com"))
            // Fallback: si falla la llamada externa, retornar la tarea sin enriquecer
            .onErrorResume(ex -> {
                log.error("Error al enriquecer tarea {}: {}. Usando fallback.",
                    task.id(), ex.getMessage());
                // Crear un usuario ficticio como fallback
                JsonPlaceholderUser fallbackUser = new JsonPlaceholderUser(
                    0, "Usuario no disponible", "N/A", "N/A", "N/A");
                return Mono.just(new EnrichedTask(task, fallbackUser, "fallback"));
            });
    }
}
```

5. Añade el endpoint de enriquecimiento al `TaskController`:

```java
// Añadir en TaskController.java

// Inyectar el servicio de enriquecimiento (añadir al constructor)
private final TaskEnrichmentService enrichmentService;

// Actualizar el constructor:
public TaskController(TaskRepository repository, TaskEnrichmentService enrichmentService) {
    this.repository = repository;
    this.enrichmentService = enrichmentService;
}

/**
 * GET /api/tasks/{id}/enriched?userId={1-10}
 * Combina datos locales de la tarea con datos de usuario de JSONPlaceholder.
 *
 * Demuestra:
 * - WebClient no bloqueante
 * - flatMap para encadenar llamadas asíncronas
 * - Fallback reactivo si la API externa falla
 */
@GetMapping("/{id}/enriched")
public Mono<EnrichedTask> getEnrichedTask(
        @PathVariable String id,
        @RequestParam(defaultValue = "1") int userId) {
    return repository.findById(id)
        .switchIfEmpty(Mono.error(new TaskNotFoundException(id)))
        // flatMap: la llamada a WebClient es asíncrona, no podemos usar map
        .flatMap(task -> enrichmentService.enrichTask(task, userId));
}
```

**Verificación:**
```bash
# Obtener el ID de una tarea existente
TASK_ID=$(curl -s http://localhost:8080/api/tasks | python3 -c "import sys,json; tasks=json.load(sys.stdin); print(tasks[0]['id'])")
echo "ID de tarea: $TASK_ID"

# Enriquecer la tarea con datos del usuario 1 de JSONPlaceholder
curl -s "http://localhost:8080/api/tasks/${TASK_ID}/enriched?userId=1" \
  | python3 -m json.tool

# Probar el fallback con un userId inválido
curl -s "http://localhost:8080/api/tasks/${TASK_ID}/enriched?userId=999" \
  | python3 -m json.tool
```

**Salida esperada para enriquecimiento exitoso:**
```json
{
  "task": {
    "id": "...",
    "title": "Configurar proyecto WebFlux",
    ...
  },
  "assignedUser": {
    "id": 1,
    "name": "Leanne Graham",
    "email": "Sincere@april.biz",
    ...
  },
  "enrichmentSource": "jsonplaceholder.typicode.com"
}
```

**Salida esperada con fallback activado:**
```json
{
  "task": { ... },
  "assignedUser": {
    "id": 0,
    "name": "Usuario no disponible",
    ...
  },
  "enrichmentSource": "fallback"
}
```

---

## 7. Validación y Pruebas

### Prueba integral de todos los endpoints

Ejecuta la siguiente secuencia de pruebas para verificar que todos los bloques funcionan correctamente:

```bash
#!/bin/bash
# Script de validación integral — guarda como validate.sh y ejecuta con: bash validate.sh

BASE_URL="http://localhost:8080"
PASS=0
FAIL=0

check() {
  local description=$1
  local expected=$2
  local actual=$3
  if echo "$actual" | grep -q "$expected"; then
    echo "✅ PASS: $description"
    ((PASS++))
  else
    echo "❌ FAIL: $description"
    echo "   Esperado: $expected"
    echo "   Obtenido: $actual"
    ((FAIL++))
  fi
}

echo "=== Bloque 1: Anotaciones y Functional Endpoints ==="
TASKS=$(curl -s $BASE_URL/api/tasks)
check "GET /api/tasks retorna array" "title" "$TASKS"

FUNC_TASKS=$(curl -s $BASE_URL/functional/tasks)
check "GET /functional/tasks retorna array" "title" "$FUNC_TASKS"

NEW_TASK=$(curl -s -o /dev/null -w "%{http_code}" -X POST $BASE_URL/api/tasks \
  -H "Content-Type: application/json" \
  -d '{"title":"Tarea de prueba","description":"Test","status":"PENDING"}')
check "POST /api/tasks retorna 201" "201" "$NEW_TASK"

echo ""
echo "=== Bloque 3: Validación y Errores ==="
VALIDATION_ERR=$(curl -s -X POST $BASE_URL/api/tasks \
  -H "Content-Type: application/json" \
  -d '{"title":"","description":"Sin título"}')
check "Validación retorna ProblemDetail" "fieldErrors" "$VALIDATION_ERR"
check "Status 400 en validación" "400" "$VALIDATION_ERR"

NOT_FOUND=$(curl -s $BASE_URL/api/tasks/id-no-existe)
check "GET tarea inexistente retorna ProblemDetail 404" "not-found" "$NOT_FOUND"

echo ""
echo "=== Bloque 4: WebClient ==="
TASK_ID=$(echo $TASKS | python3 -c "import sys,json; print(json.load(sys.stdin)[0]['id'])")
ENRICHED=$(curl -s "$BASE_URL/api/tasks/$TASK_ID/enriched?userId=1")
check "Tarea enriquecida contiene assignedUser" "assignedUser" "$ENRICHED"
check "Fuente de enriquecimiento presente" "enrichmentSource" "$ENRICHED"

echo ""
echo "=== Resumen ==="
echo "✅ Passed: $PASS | ❌ Failed: $FAIL"
```

```bash
chmod +x validate.sh && bash validate.sh
```

**Salida esperada:**
```
=== Bloque 1: Anotaciones y Functional Endpoints ===
✅ PASS: GET /api/tasks retorna array
✅ PASS: GET /functional/tasks retorna array
✅ PASS: POST /api/tasks retorna 201

=== Bloque 3: Validación y Errores ===
✅ PASS: Validación retorna ProblemDetail
✅ PASS: Status 400 en validación
✅ PASS: GET tarea inexistente retorna ProblemDetail 404

=== Bloque 4: WebClient ===
✅ PASS: Tarea enriquecida contiene assignedUser
✅ PASS: Fuente de enriquecimiento presente

=== Resumen ===
✅ Passed: 8 | ❌ Failed: 0
```

### Prueba del endpoint SSE con curl

```bash
# Conectar al stream SSE y observar los eventos durante 10 segundos
timeout 10 curl -N -H "Accept: text/event-stream" http://localhost:8080/api/tasks/stream
```

---

## 8. Resolución de Problemas

### Problema 1: `WebExchangeBindException` no es interceptada por `@RestControllerAdvice`

**Síntomas:** Al enviar datos de entrada inválidos, el servidor retorna un error 400 genérico de Spring (sin el formato `ProblemDetail` esperado) o un stack trace en la respuesta. El `GlobalErrorHandler` no parece activarse.

**Causa:** Spring WebFlux requiere que el `@RestControllerAdvice` esté en un paquete que sea escaneado por el contexto de la aplicación. Si el paquete `exception` no está bajo el paquete base declarado en `@SpringBootApplication`, los beans no se detectan. También puede ocurrir si hay múltiples `WebExceptionHandler` registrados con mayor precedencia.

**Solución:**
```bash
# Verificar que el paquete base de la aplicación cubre todos los subpaquetes
# La clase principal debe estar en com.curso.reactivo.tasks (raíz)
# y no en com.curso.reactivo.tasks.app u otro subpaquete más profundo

# Verificar la estructura de paquetes
find src/main/java -name "*.java" | head -20

# Si el problema persiste, añadir @ComponentScan explícito a la clase principal:
```
```java
// En TaskManagementApiApplication.java — añadir si es necesario
@SpringBootApplication
@ComponentScan(basePackages = "com.curso.reactivo.tasks")
public class TaskManagementApiApplication {
    public static void main(String[] args) {
        SpringApplication.run(TaskManagementApiApplication.class, args);
    }
}
```
```bash
# Reiniciar y verificar que los beans se registran:
mvn spring-boot:run 2>&1 | grep -i "GlobalErrorHandler\|ControllerAdvice"
```

---

### Problema 2: El endpoint SSE retorna JSON en lugar de `text/event-stream` o la conexión se cierra inmediatamente

**Síntomas:** Al llamar a `/api/tasks/stream`, el cliente recibe todos los elementos en un solo array JSON `[{...},{...}]` en lugar de eventos SSE separados, o la conexión se cierra antes de emitir todos los elementos. En algunos casos, curl muestra `curl: (56) Recv failure`.

**Causa:** Existen dos causas posibles: (1) El cliente no está enviando el header `Accept: text/event-stream`, por lo que WebFlux aplica negociación de contenido y sirve JSON. (2) El `Flux` termina antes de que el cliente se conecte porque `delayElements` no se aplica correctamente cuando el Flux es frío y ya está completado.

**Solución:**
```bash
# Causa 1: Siempre incluir el header Accept correcto
curl -N \
  -H "Accept: text/event-stream" \
  -H "Cache-Control: no-cache" \
  http://localhost:8080/api/tasks/stream

# Causa 2: Verificar que el endpoint tiene produces = TEXT_EVENT_STREAM_VALUE
# y que delayElements se aplica ANTES de la suscripción
```
```java
// Verificar que el endpoint en TaskController tiene la configuración correcta:
@GetMapping(value = "/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
public Flux<Task> streamTasks() {
    // delayElements debe estar en el pipeline, no en el repositorio
    return repository.findAll()
        .delayElements(Duration.ofSeconds(1))
        // repeat() hace el Flux infinito si se desea un stream continuo
        // .repeat()
        ;
}
```
```bash
# Verificar en los logs que Netty está usando modo streaming
mvn spring-boot:run 2>&1 | grep -i "text/event-stream\|SSE"
```

---

## 9. Limpieza del Entorno

Al finalizar la práctica, la aplicación no usa Docker ni servicios externos persistentes, por lo que la limpieza es mínima:

```bash
# 1. Detener la aplicación Spring Boot (Ctrl+C en la terminal donde corre)
# O si corre en background:
pkill -f "spring-boot:run" 2>/dev/null || true

# 2. Limpiar artefactos de compilación Maven
cd task-management-api
mvn clean

# 3. (Opcional) Eliminar el proyecto si no se continuará en Lab 03
# ADVERTENCIA: El Lab 03 extiende este proyecto. Conservar si se continuará.
# cd .. && rm -rf task-management-api

# 4. Verificar que no hay procesos Java residuales en el puerto 8080
lsof -ti:8080 | xargs kill -9 2>/dev/null || echo "Puerto 8080 libre"
```

> ⚠️ **Importante:** Si vas a continuar con el **Lab 03**, **NO elimines el proyecto**. El Lab 03 extiende esta misma base añadiendo persistencia con R2DBC y MongoDB. Conserva la estructura de paquetes y el código de esta práctica.

---

## 10. Resumen

En esta práctica construiste una **Task Management API** completa que demuestra los cuatro pilares del módulo 2:

| Bloque | Concepto | Implementado |
|--------|----------|--------------|
| **1** | Anotaciones vs Functional Endpoints | `@RestController` en `/api/tasks` + `RouterFunction` en `/functional/tasks` |
| **2** | Negociación de contenido / SSE | Endpoints `produces = TEXT_EVENT_STREAM_VALUE` con `Flux.delayElements` |
| **3** | Validación + ProblemDetail (RFC 7807) | `@Valid` + `@RestControllerAdvice` + `WebExchangeBindException` |
| **4** | WebClient no bloqueante | Timeout 3s + `retryWhen` + `onErrorResume` fallback |

### Conceptos clave reforzados

- **Ambos modelos coexisten** en el mismo proyecto bajo el mismo `DispatcherHandler`; la elección es arquitectónica, no técnica.
- **`switchIfEmpty` vs `onErrorResume`**: `switchIfEmpty` reacciona a Monos vacíos; `onErrorResume` reacciona a señales de error. Ambos son esenciales para flujos reactivos robustos.
- **`flatMap` para operaciones asíncronas**: cuando la transformación dentro del pipeline devuelve un `Mono`/`Flux`, se debe usar `flatMap`, no `map`.
- **El fallback reactivo** con `onErrorResume` garantiza que un fallo en un servicio externo no interrumpa el flujo principal.
- **`block()` sigue prohibido** en cualquier controller, handler o pipeline reactivo en producción.

### Recursos adicionales

- [Spring WebFlux Reference — Annotated Controllers](https://docs.spring.io/spring-framework/reference/web/webflux/controller.html)
- [Spring WebFlux Reference — Functional Endpoints](https://docs.spring.io/spring-framework/reference/web/webflux-functional.html)
- [WebClient Reference Documentation](https://docs.spring.io/spring-framework/reference/web/webflux-webclient.html)
- [ProblemDetail — Spring Framework 6](https://docs.spring.io/spring-framework/reference/web/webmvc/mvc-ann-rest-exceptions.html)
- [Server-Sent Events con Spring WebFlux — Baeldung](https://www.baeldung.com/spring-server-sent-events)
- [Bean Validation en WebFlux — Documentación oficial](https://docs.spring.io/spring-framework/reference/web/webflux/controller/ann-validation.html)

---
*Lab 02-00-01 — Curso de Programación Reactiva con Spring WebFlux | Módulo 2*
