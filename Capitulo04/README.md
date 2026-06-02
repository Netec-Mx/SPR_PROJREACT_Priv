---LAB_START---
LAB_ID: 04-00-01
---MARKDOWN---
# Tests + Métricas + Troubleshooting

## Metadatos

| Campo         | Valor                                      |
|---------------|--------------------------------------------|
| **Duración**  | 100 minutos                                |
| **Complejidad** | Alta                                     |
| **Nivel Bloom** | Aplicar (Apply)                          |
| **Lab previo** | Lab 03-00-01 (Persistencia Reactiva)      |
| **Versión Spring Boot** | 3.3.x                          |

---

## Descripción General

En este laboratorio ampliarás el proyecto del catálogo reactivo construido en el Lab 03 añadiendo una suite de pruebas completa y un stack de observabilidad. Aprenderás a verificar el comportamiento de pipelines reactivos usando `StepVerifier` y `WebTestClient`, a exponer métricas de negocio con Micrometer y visualizarlas en Grafana, y a identificar y corregir tres antipatrones reactivos críticos que aparecen con frecuencia en código de producción. El laboratorio está dividido en cuatro bloques progresivos que deben completarse en orden.

> ⚠️ **Antipatrón crítico**: `block()`, `blockFirst()` y `blockLast()` **nunca** deben usarse dentro de un controller WebFlux ni dentro de un pipeline reactivo en producción. Solo son aceptables en el método `main()` o en tests con justificación explícita. Este principio se refuerza en el Bloque 4 de este lab.

---

## Objetivos de Aprendizaje

Al finalizar este laboratorio serás capaz de:

- [ ] Escribir tests unitarios de pipelines reactivos con `StepVerifier`, verificando elementos emitidos, errores, completación y comportamiento temporal con `VirtualTimeScheduler`.
- [ ] Implementar tests de integración de endpoints WebFlux con `WebTestClient`, incluyendo verificación de streams SSE y respuestas paginadas.
- [ ] Configurar Spring Boot Actuator con health checks personalizados reactivos y métricas de negocio usando `Counter` y `Timer` de Micrometer, exponiendo `/actuator/prometheus`.
- [ ] Identificar, diagnosticar y corregir antipatrones reactivos: bloqueo en el event loop, uso incorrecto de schedulers y pérdida de contexto reactivo.
- [ ] Aplicar `ReactorContext` para propagar información transversal (trace ID, usuario) a través del pipeline sin pasar parámetros explícitos.

---

## Prerrequisitos

### Conocimiento previo
- Haber completado Lab 03-00-01 o tener el proyecto base provisto por el instructor.
- Comprensión de `Mono` y `Flux` del Lab 01 (operadores de transformación y manejo de errores).
- Experiencia básica con JUnit 5 y Mockito en contexto Spring Boot.
- Nociones básicas de métricas de aplicación (latencia, throughput, error rate).

### Acceso y herramientas
- JDK 21 LTS instalado y configurado en `JAVA_HOME`.
- Apache Maven 3.9.x disponible en el PATH.
- Docker Desktop / Docker Engine 24.x en ejecución.
- IntelliJ IDEA 2024.1+ (Community o Ultimate).
- Proyecto del Lab 03 compilando correctamente (`mvn clean package -DskipTests`).

---

## Entorno del Laboratorio

### Hardware recomendado

| Recurso    | Mínimo       | Recomendado  |
|------------|-------------|--------------|
| CPU        | 4 núcleos   | 8 núcleos    |
| RAM        | 8 GB        | 16 GB        |
| Disco libre | 5 GB       | 10 GB        |

### Software y versiones

| Herramienta          | Versión      | Uso en este lab                        |
|----------------------|-------------|----------------------------------------|
| Spring Boot          | 3.3.x       | Framework base                         |
| reactor-test         | 3.6.x       | StepVerifier, VirtualTimeScheduler     |
| Spring Boot Actuator | 3.3.x       | Health checks, métricas                |
| Micrometer           | 1.13.x      | Counter, Timer, Gauge                  |
| Prometheus           | 2.51+       | Scraping de métricas (Docker)          |
| Grafana              | 10.x        | Visualización (Docker)                 |
| Mockito              | 5.x         | Mocking en tests unitarios             |

### Verificación del entorno

Ejecuta los siguientes comandos antes de comenzar:

```bash
# Verificar Java 21
java -version
# Expected: openjdk version "21.x.x"

# Verificar Maven
mvn -version
# Expected: Apache Maven 3.9.x

# Verificar Docker
docker info | grep "Server Version"
docker compose version

# Verificar que el proyecto del Lab 03 compila
cd ~/labs/catalog-service
mvn clean package -DskipTests -q
echo "Build OK: $?"
```

### Preparar el proyecto base

Si no tienes el proyecto del Lab 03, usa el proyecto base provisto:

```bash
# Clonar proyecto base (si aplica)
git clone https://github.com/curso-webflux/lab04-base.git catalog-service
cd catalog-service

# Verificar estructura
ls src/main/java/com/ejemplo/catalog/
# Expected: controller/, service/, repository/, model/, config/
```

---

## Instrucciones Paso a Paso

---

### Bloque 1: Tests Unitarios con StepVerifier (25 minutos)

**Objetivo del bloque**: Escribir tests unitarios que verifiquen el comportamiento de los servicios reactivos del catálogo usando `StepVerifier`, incluyendo pruebas con tiempo virtual.

---

#### Paso 1.1 — Verificar dependencias de test en el POM

**Objetivo**: Asegurar que `reactor-test` y las dependencias de test estén disponibles.

**Instrucciones**:

1. Abre `pom.xml` en la raíz del proyecto.
2. Verifica que existan las siguientes dependencias (agrégalas si faltan):

```xml
<!-- pom.xml — sección <dependencies> -->

<!-- reactor-test: StepVerifier y VirtualTimeScheduler -->
<dependency>
    <groupId>io.projectreactor</groupId>
    <artifactId>reactor-test</artifactId>
    <scope>test</scope>
    <!-- Sin versión: gestionada por Spring Boot BOM -->
</dependency>

<!-- Spring Boot Test: WebTestClient, @WebFluxTest, @SpringBootTest -->
<dependency>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-starter-test</artifactId>
    <scope>test</scope>
</dependency>

<!-- Mockito para mocking reactivo -->
<dependency>
    <groupId>org.mockito</groupId>
    <artifactId>mockito-core</artifactId>
    <scope>test</scope>
</dependency>
```

3. Ejecuta `mvn dependency:resolve -q` para confirmar la resolución.

**Salida esperada**:
```
BUILD SUCCESS
```

**Verificación**:
```bash
mvn dependency:tree | grep "reactor-test"
# Expected: [INFO]    \- io.projectreactor:reactor-test:jar:3.6.x:test
```

---

#### Paso 1.2 — Crear tests unitarios del servicio de catálogo

**Objetivo**: Verificar el comportamiento de `ProductService` con `StepVerifier`.

**Instrucciones**:

1. Crea el archivo de test en la ruta `src/test/java/com/ejemplo/catalog/service/ProductServiceTest.java`:

```java
package com.ejemplo.catalog.service;

import com.ejemplo.catalog.model.Product;
import com.ejemplo.catalog.repository.ProductRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.math.BigDecimal;
import java.time.Duration;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@DisplayName("ProductService — Tests Unitarios Reactivos")
class ProductServiceTest {

    @Mock
    private ProductRepository productRepository;

    @InjectMocks
    private ProductService productService;

    private Product productoDePrueba;

    @BeforeEach
    void setUp() {
        productoDePrueba = new Product();
        productoDePrueba.setId("prod-001");
        productoDePrueba.setName("Laptop Pro");
        productoDePrueba.setPrice(new BigDecimal("1299.99"));
        productoDePrueba.setStock(10);
    }

    // ---------------------------------------------------------------
    // Tests de flujos exitosos
    // ---------------------------------------------------------------

    @Test
    @DisplayName("findAll() debe emitir todos los productos y completar")
    void findAll_debeEmitirTodosLosProductosYCompletar() {
        // Arrange: el repositorio devuelve dos productos
        Product producto2 = new Product();
        producto2.setId("prod-002");
        producto2.setName("Teclado Mecánico");
        producto2.setPrice(new BigDecimal("89.99"));
        producto2.setStock(25);

        when(productRepository.findAll())
            .thenReturn(Flux.just(productoDePrueba, producto2));

        // Act + Assert con StepVerifier
        StepVerifier.create(productService.findAll())
            // Verificar primer elemento con predicado
            .expectNextMatches(p -> "prod-001".equals(p.getId())
                                 && "Laptop Pro".equals(p.getName()))
            // Verificar segundo elemento con predicado
            .expectNextMatches(p -> "prod-002".equals(p.getId()))
            // Verificar señal onComplete
            .verifyComplete();
    }

    @Test
    @DisplayName("findById() debe emitir el producto cuando existe")
    void findById_debeEmitirProductoCuandoExiste() {
        when(productRepository.findById("prod-001"))
            .thenReturn(Mono.just(productoDePrueba));

        StepVerifier.create(productService.findById("prod-001"))
            .expectNext(productoDePrueba)
            .verifyComplete();
    }

    @Test
    @DisplayName("findById() debe emitir error cuando el producto no existe")
    void findById_debeEmitirErrorCuandoProductoNoExiste() {
        when(productRepository.findById("no-existe"))
            .thenReturn(Mono.empty());

        // El servicio debe transformar Mono.empty() en un error de negocio
        StepVerifier.create(productService.findById("no-existe"))
            .expectErrorMatches(ex ->
                ex instanceof ProductNotFoundException &&
                ex.getMessage().contains("no-existe"))
            .verify();
    }

    @Test
    @DisplayName("save() debe emitir el producto guardado")
    void save_debeEmitirProductoGuardado() {
        when(productRepository.save(any(Product.class)))
            .thenReturn(Mono.just(productoDePrueba));

        StepVerifier.create(productService.save(productoDePrueba))
            .expectNextMatches(p -> p.getId() != null)
            .verifyComplete();
    }

    // ---------------------------------------------------------------
    // Test con flujo vacío
    // ---------------------------------------------------------------

    @Test
    @DisplayName("findAll() debe completar sin emitir elementos cuando no hay productos")
    void findAll_debeCompletarSinElementosCuandoRepositorioEstaVacio() {
        when(productRepository.findAll()).thenReturn(Flux.empty());

        StepVerifier.create(productService.findAll())
            .expectNextCount(0)   // cero elementos emitidos
            .verifyComplete();
    }

    // ---------------------------------------------------------------
    // Test de conteo de elementos
    // ---------------------------------------------------------------

    @Test
    @DisplayName("findAll() debe emitir exactamente N elementos")
    void findAll_debeEmitirExactamenteNElementos() {
        Flux<Product> productosSimulados = Flux.range(1, 5)
            .map(i -> {
                Product p = new Product();
                p.setId("prod-00" + i);
                p.setName("Producto " + i);
                p.setPrice(BigDecimal.TEN);
                p.setStock(i * 10);
                return p;
            });

        when(productRepository.findAll()).thenReturn(productosSimulados);

        StepVerifier.create(productService.findAll())
            .expectNextCount(5)   // exactamente 5 elementos
            .verifyComplete();
    }
}
```

2. Asegúrate de que `ProductService` tenga el método `findById` que transforme `Mono.empty()` en error. Si no existe, agrégalo:

```java
// En ProductService.java
public Mono<Product> findById(String id) {
    return productRepository.findById(id)
        .switchIfEmpty(Mono.error(
            new ProductNotFoundException("Producto no encontrado: " + id)));
}
```

3. Crea la excepción de negocio si no existe:

```java
// src/main/java/com/ejemplo/catalog/service/ProductNotFoundException.java
package com.ejemplo.catalog.service;

public class ProductNotFoundException extends RuntimeException {
    public ProductNotFoundException(String message) {
        super(message);
    }
}
```

**Salida esperada**:
```
[INFO] Tests run: 5, Failures: 0, Errors: 0, Skipped: 0
```

**Verificación**:
```bash
mvn test -pl . -Dtest=ProductServiceTest -q
# Expected: BUILD SUCCESS con 5 tests pasando
```

---

#### Paso 1.3 — Test con VirtualTimeScheduler para operadores temporales

**Objetivo**: Verificar pipelines que usan `delayElements` o `timeout` sin esperar tiempo real.

**Instrucciones**:

1. Crea el archivo `src/test/java/com/ejemplo/catalog/service/ProductServiceVirtualTimeTest.java`:

```java
package com.ejemplo.catalog.service;

import com.ejemplo.catalog.model.Product;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.math.BigDecimal;
import java.time.Duration;

@DisplayName("ProductService — Tests con VirtualTimeScheduler")
class ProductServiceVirtualTimeTest {

    /**
     * Simula un Flux con delayElements — en producción podría ser
     * un stream de eventos con cadencia controlada.
     */
    private Flux<String> streamConRetardo() {
        return Flux.just("Laptop", "Teclado", "Monitor")
            .delayElements(Duration.ofSeconds(1));
    }

    /**
     * Simula un Mono con timeout — operación de base de datos lenta.
     */
    private Mono<Product> operacionConTimeout() {
        Product p = new Product();
        p.setId("prod-slow");
        p.setName("Producto Lento");
        p.setPrice(BigDecimal.TEN);
        p.setStock(1);

        return Mono.just(p)
            .delayElement(Duration.ofSeconds(3));
    }

    @Test
    @DisplayName("streamConRetardo() debe emitir 3 elementos en 3 segundos virtuales")
    void streamConRetardo_debeEmitirElementosSinEsperarTiempoReal() {
        // withVirtualTime recibe un Supplier para retrasar la creación del Flux
        // hasta que el scheduler virtual esté activo
        StepVerifier.withVirtualTime(() -> streamConRetardo())
            .expectSubscription()
            // Avanzar el reloj virtual 3 segundos de golpe
            .thenAwait(Duration.ofSeconds(3))
            .expectNext("Laptop", "Teclado", "Monitor")
            .verifyComplete();
    }

    @Test
    @DisplayName("streamConRetardo() debe emitir un elemento por segundo virtual")
    void streamConRetardo_debeEmitirUnElementoPorSegundoVirtual() {
        StepVerifier.withVirtualTime(() -> streamConRetardo())
            .expectSubscription()
            // Avanzar segundo a segundo y verificar cada elemento
            .thenAwait(Duration.ofSeconds(1))
            .expectNext("Laptop")
            .thenAwait(Duration.ofSeconds(1))
            .expectNext("Teclado")
            .thenAwait(Duration.ofSeconds(1))
            .expectNext("Monitor")
            .verifyComplete();
    }

    @Test
    @DisplayName("operacionConTimeout() debe completar dentro del límite virtual")
    void operacionConTimeout_debeCompletarDentroDelLimite() {
        StepVerifier.withVirtualTime(() ->
                operacionConTimeout().timeout(Duration.ofSeconds(5)))
            .expectSubscription()
            .thenAwait(Duration.ofSeconds(3))
            .expectNextMatches(p -> "prod-slow".equals(p.getId()))
            .verifyComplete();
    }

    @Test
    @DisplayName("operacionConTimeout() debe emitir TimeoutException si supera el límite")
    void operacionConTimeout_debeEmitirTimeoutExceptionSiSuperaLimite() {
        StepVerifier.withVirtualTime(() ->
                operacionConTimeout().timeout(Duration.ofSeconds(2)))
            .expectSubscription()
            .thenAwait(Duration.ofSeconds(2))
            .expectError(java.util.concurrent.TimeoutException.class)
            .verify();
    }
}
```

> **Nota importante**: Siempre usa `withVirtualTime(() -> crearFlux())` con un `Supplier` (lambda), **no** `withVirtualTime(crearFlux())`. Si el `Flux` se crea antes de que el scheduler virtual esté activo, los retardos se ejecutarán en tiempo real.

**Salida esperada**:
```
[INFO] Tests run: 4, Failures: 0, Errors: 0, Skipped: 0
[INFO] BUILD SUCCESS
# Tiempo total: < 2 segundos (sin tiempo virtual real esperado)
```

**Verificación**:
```bash
mvn test -Dtest=ProductServiceVirtualTimeTest -q
# El test completo debe tardar menos de 5 segundos de reloj real
```

---

### Bloque 2: Tests de Integración con WebTestClient (25 minutos)

**Objetivo del bloque**: Implementar tests de integración para los endpoints HTTP del catálogo, incluyendo verificación del stream SSE.

---

#### Paso 2.1 — Test de slice con @WebFluxTest

**Objetivo**: Probar el controller de productos aislado, mockeando el servicio.

**Instrucciones**:

1. Crea `src/test/java/com/ejemplo/catalog/controller/ProductControllerSliceTest.java`:

```java
package com.ejemplo.catalog.controller;

import com.ejemplo.catalog.model.Product;
import com.ejemplo.catalog.service.ProductNotFoundException;
import com.ejemplo.catalog.service.ProductService;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.reactive.WebFluxTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.reactive.server.WebTestClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.time.Duration;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

// @WebFluxTest carga solo la capa web: controllers, filters, exception handlers.
// NO carga repositorios ni servicios reales → los mocks son obligatorios.
@WebFluxTest(ProductController.class)
@DisplayName("ProductController — Tests de Slice (@WebFluxTest)")
class ProductControllerSliceTest {

    @Autowired
    private WebTestClient webTestClient;

    @MockBean
    private ProductService productService;

    // ---------------------------------------------------------------
    // GET /api/products
    // ---------------------------------------------------------------

    @Test
    @DisplayName("GET /api/products debe retornar 200 con lista de productos")
    void getAll_debeRetornar200ConListaDeProductos() {
        Product p1 = buildProduct("prod-001", "Laptop Pro", "1299.99", 10);
        Product p2 = buildProduct("prod-002", "Teclado Mecánico", "89.99", 25);

        when(productService.findAll()).thenReturn(Flux.just(p1, p2));

        webTestClient.get()
            .uri("/api/products")
            .accept(MediaType.APPLICATION_JSON)
            .exchange()
            .expectStatus().isOk()
            .expectHeader().contentType(MediaType.APPLICATION_JSON)
            .expectBodyList(Product.class)
            .hasSize(2);
    }

    // ---------------------------------------------------------------
    // GET /api/products/{id}
    // ---------------------------------------------------------------

    @Test
    @DisplayName("GET /api/products/{id} debe retornar 200 cuando el producto existe")
    void getById_debeRetornar200CuandoProductoExiste() {
        Product p = buildProduct("prod-001", "Laptop Pro", "1299.99", 10);
        when(productService.findById("prod-001")).thenReturn(Mono.just(p));

        webTestClient.get()
            .uri("/api/products/prod-001")
            .accept(MediaType.APPLICATION_JSON)
            .exchange()
            .expectStatus().isOk()
            .expectBody(Product.class)
            .value(product -> {
                assert "prod-001".equals(product.getId());
                assert "Laptop Pro".equals(product.getName());
            });
    }

    @Test
    @DisplayName("GET /api/products/{id} debe retornar 404 cuando el producto no existe")
    void getById_debeRetornar404CuandoProductoNoExiste() {
        when(productService.findById("no-existe"))
            .thenReturn(Mono.error(
                new ProductNotFoundException("Producto no encontrado: no-existe")));

        webTestClient.get()
            .uri("/api/products/no-existe")
            .accept(MediaType.APPLICATION_JSON)
            .exchange()
            .expectStatus().isNotFound();
    }

    // ---------------------------------------------------------------
    // POST /api/products
    // ---------------------------------------------------------------

    @Test
    @DisplayName("POST /api/products debe retornar 201 con el producto creado")
    void create_debeRetornar201ConProductoCreado() {
        Product input = buildProduct(null, "Nuevo Producto", "499.99", 5);
        Product saved  = buildProduct("prod-003", "Nuevo Producto", "499.99", 5);

        when(productService.save(any(Product.class))).thenReturn(Mono.just(saved));

        webTestClient.post()
            .uri("/api/products")
            .contentType(MediaType.APPLICATION_JSON)
            .bodyValue(input)
            .exchange()
            .expectStatus().isCreated()
            .expectBody(Product.class)
            .value(p -> assert "prod-003".equals(p.getId()));
    }

    // ---------------------------------------------------------------
    // GET /api/products/stream — Server-Sent Events
    // ---------------------------------------------------------------

    @Test
    @DisplayName("GET /api/products/stream debe retornar stream SSE con productos")
    void stream_debeRetornarStreamSSEConProductos() {
        Product p1 = buildProduct("prod-001", "Laptop Pro", "1299.99", 10);
        Product p2 = buildProduct("prod-002", "Teclado Mecánico", "89.99", 25);

        // El stream SSE emite con un pequeño retardo entre elementos
        when(productService.findAll())
            .thenReturn(Flux.just(p1, p2).delayElements(Duration.ofMillis(100)));

        webTestClient
            // Aumentar timeout para streams SSE
            .mutate().responseTimeout(Duration.ofSeconds(10)).build()
            .get()
            .uri("/api/products/stream")
            .accept(MediaType.TEXT_EVENT_STREAM)
            .exchange()
            .expectStatus().isOk()
            .expectHeader().contentTypeCompatibleWith(MediaType.TEXT_EVENT_STREAM)
            .expectBodyList(Product.class)
            .hasSize(2);
    }

    // ---------------------------------------------------------------
    // Método utilitario
    // ---------------------------------------------------------------

    private Product buildProduct(String id, String name, String price, int stock) {
        Product p = new Product();
        p.setId(id);
        p.setName(name);
        p.setPrice(new BigDecimal(price));
        p.setStock(stock);
        return p;
    }
}
```

**Salida esperada**:
```
[INFO] Tests run: 5, Failures: 0, Errors: 0, Skipped: 0
```

---

#### Paso 2.2 — Test de integración completo con @SpringBootTest

**Objetivo**: Levantar el contexto completo de Spring y verificar el flujo end-to-end con base de datos embebida/mockeada.

**Instrucciones**:

1. Crea `src/test/java/com/ejemplo/catalog/integration/ProductIntegrationTest.java`:

```java
package com.ejemplo.catalog.integration;

import com.ejemplo.catalog.model.Product;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.reactive.server.WebTestClient;

import java.math.BigDecimal;

// @SpringBootTest levanta el contexto completo en puerto aleatorio
// Usar perfil "test" para apuntar a base de datos H2/embebida
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles("test")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@DisplayName("Integración completa — ProductController + ProductService + Repository")
class ProductIntegrationTest {

    @Autowired
    private WebTestClient webTestClient;

    private static String productoIdCreado;

    @Test
    @Order(1)
    @DisplayName("POST /api/products — crear producto retorna 201")
    void crearProducto_retorna201() {
        Product nuevo = new Product();
        nuevo.setName("Monitor UltraWide");
        nuevo.setPrice(new BigDecimal("799.99"));
        nuevo.setStock(8);

        webTestClient.post()
            .uri("/api/products")
            .contentType(MediaType.APPLICATION_JSON)
            .bodyValue(nuevo)
            .exchange()
            .expectStatus().isCreated()
            .expectBody(Product.class)
            .value(p -> {
                productoIdCreado = p.getId();
                assert p.getId() != null : "El ID no debe ser null después de guardar";
                assert "Monitor UltraWide".equals(p.getName());
            });
    }

    @Test
    @Order(2)
    @DisplayName("GET /api/products/{id} — recuperar producto recién creado")
    void recuperarProducto_retorna200() {
        if (productoIdCreado == null) return; // skip si el test anterior falló

        webTestClient.get()
            .uri("/api/products/" + productoIdCreado)
            .accept(MediaType.APPLICATION_JSON)
            .exchange()
            .expectStatus().isOk()
            .expectBody()
            .jsonPath("$.name").isEqualTo("Monitor UltraWide")
            .jsonPath("$.stock").isEqualTo(8);
    }

    @Test
    @Order(3)
    @DisplayName("GET /api/products — lista incluye el producto creado")
    void listarProductos_incluyeProductoCreado() {
        webTestClient.get()
            .uri("/api/products")
            .accept(MediaType.APPLICATION_JSON)
            .exchange()
            .expectStatus().isOk()
            .expectBodyList(Product.class)
            .value(lista -> assert lista.size() >= 1);
    }

    @Test
    @Order(4)
    @DisplayName("GET /api/products/no-existe — retorna 404")
    void productNoExistente_retorna404() {
        webTestClient.get()
            .uri("/api/products/id-que-no-existe-xyz")
            .accept(MediaType.APPLICATION_JSON)
            .exchange()
            .expectStatus().isNotFound();
    }
}
```

2. Crea el perfil de test `src/test/resources/application-test.yml`:

```yaml
# Perfil de test: base de datos en memoria (H2 con R2DBC)
spring:
  r2dbc:
    url: r2dbc:h2:mem:///testdb;DB_CLOSE_DELAY=-1
    username: sa
    password:
  sql:
    init:
      mode: always
      schema-locations: classpath:schema-test.sql

logging:
  level:
    com.ejemplo.catalog: DEBUG
    io.r2dbc: DEBUG
```

3. Crea el schema de test `src/test/resources/schema-test.sql`:

```sql
-- Schema para base de datos H2 en tests
DROP TABLE IF EXISTS products;

CREATE TABLE products (
    id      VARCHAR(36) PRIMARY KEY DEFAULT RANDOM_UUID(),
    name    VARCHAR(255) NOT NULL,
    price   DECIMAL(10,2) NOT NULL,
    stock   INT NOT NULL DEFAULT 0
);
```

**Salida esperada**:
```
[INFO] Tests run: 4, Failures: 0, Errors: 0, Skipped: 0
[INFO] BUILD SUCCESS
```

**Verificación**:
```bash
mvn test -Dtest=ProductControllerSliceTest,ProductIntegrationTest -q
```

---

### Bloque 3: Actuator, Micrometer y Observabilidad (25 minutos)

**Objetivo del bloque**: Configurar Spring Boot Actuator con health checks personalizados reactivos, métricas de negocio con Micrometer, y levantar Prometheus + Grafana para visualización.

---

#### Paso 3.1 — Configurar Spring Boot Actuator y Micrometer

**Instrucciones**:

1. Agrega las dependencias en `pom.xml`:

```xml
<!-- Spring Boot Actuator -->
<dependency>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-starter-actuator</artifactId>
</dependency>

<!-- Micrometer Registry para Prometheus -->
<dependency>
    <groupId>io.micrometer</groupId>
    <artifactId>micrometer-registry-prometheus</artifactId>
    <scope>runtime</scope>
</dependency>
```

2. Configura Actuator en `src/main/resources/application.yml`:

```yaml
management:
  endpoints:
    web:
      exposure:
        include: health, info, metrics, prometheus, loggers
      base-path: /actuator
  endpoint:
    health:
      show-details: always
      show-components: always
    prometheus:
      enabled: true
  metrics:
    tags:
      application: catalog-service
      environment: ${SPRING_PROFILES_ACTIVE:local}
    distribution:
      percentiles-histogram:
        http.server.requests: true
      percentiles:
        http.server.requests: 0.5, 0.95, 0.99

info:
  app:
    name: Catalog Service
    version: "@project.version@"
    description: API reactiva de catálogo de productos
```

---

#### Paso 3.2 — Implementar Health Indicator reactivo

**Objetivo**: Crear un `ReactiveHealthIndicator` que verifique la conectividad con la base de datos de forma no bloqueante.

**Instrucciones**:

1. Crea `src/main/java/com/ejemplo/catalog/health/DatabaseHealthIndicator.java`:

```java
package com.ejemplo.catalog.health;

import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.ReactiveHealthIndicator;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.time.Duration;

/**
 * Health indicator reactivo para la base de datos R2DBC.
 * Implementa ReactiveHealthIndicator (no HealthIndicator bloqueante)
 * para mantener el modelo no bloqueante de WebFlux.
 */
@Component("database")
public class DatabaseHealthIndicator implements ReactiveHealthIndicator {

    private final DatabaseClient databaseClient;

    public DatabaseHealthIndicator(DatabaseClient databaseClient) {
        this.databaseClient = databaseClient;
    }

    @Override
    public Mono<Health> health() {
        return checkDatabaseConnection()
            .map(latencyMs -> Health.up()
                .withDetail("latencyMs", latencyMs)
                .withDetail("database", "PostgreSQL/R2DBC")
                .build())
            .onErrorResume(ex -> Mono.just(Health.down()
                .withDetail("error", ex.getMessage())
                .withDetail("database", "PostgreSQL/R2DBC")
                .build()))
            // Timeout de 3 segundos para el health check
            .timeout(Duration.ofSeconds(3),
                Mono.just(Health.down()
                    .withDetail("error", "Health check timeout after 3s")
                    .build()));
    }

    private Mono<Long> checkDatabaseConnection() {
        long inicio = System.currentTimeMillis();
        return databaseClient.sql("SELECT 1")
            .fetch()
            .one()
            .map(result -> System.currentTimeMillis() - inicio);
    }
}
```

---

#### Paso 3.3 — Agregar métricas de negocio con Micrometer

**Objetivo**: Instrumentar `ProductService` con `Counter` y `Timer` para medir operaciones de negocio.

**Instrucciones**:

1. Modifica `ProductService.java` para agregar métricas:

```java
package com.ejemplo.catalog.service;

import com.ejemplo.catalog.model.Product;
import com.ejemplo.catalog.repository.ProductRepository;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.concurrent.atomic.AtomicInteger;

@Service
public class ProductService {

    private final ProductRepository productRepository;

    // Métricas de negocio
    private final Counter productosConsultadosCounter;
    private final Counter productosNoEncontradosCounter;
    private final Counter productosGuardadosCounter;
    private final Timer busquedaTimer;
    private final AtomicInteger productosEnMemoria = new AtomicInteger(0);

    public ProductService(ProductRepository productRepository,
                          MeterRegistry meterRegistry) {
        this.productRepository = productRepository;

        // Counter: número total de consultas por ID
        this.productosConsultadosCounter = Counter.builder("catalog.products.consulted")
            .description("Total de productos consultados por ID")
            .tag("operation", "findById")
            .register(meterRegistry);

        // Counter: número de búsquedas que resultaron en not found
        this.productosNoEncontradosCounter = Counter.builder("catalog.products.not_found")
            .description("Total de consultas que resultaron en producto no encontrado")
            .tag("operation", "findById")
            .register(meterRegistry);

        // Counter: número de productos guardados
        this.productosGuardadosCounter = Counter.builder("catalog.products.saved")
            .description("Total de productos guardados")
            .tag("operation", "save")
            .register(meterRegistry);

        // Timer: latencia de búsqueda por ID
        this.busquedaTimer = Timer.builder("catalog.products.search.duration")
            .description("Latencia de búsqueda de producto por ID")
            .tag("operation", "findById")
            .publishPercentiles(0.5, 0.95, 0.99)
            .register(meterRegistry);

        // Gauge: productos actualmente cargados en memoria (ejemplo ilustrativo)
        io.micrometer.core.instrument.Gauge.builder(
                "catalog.products.in_memory",
                productosEnMemoria,
                AtomicInteger::get)
            .description("Productos actualmente en caché de memoria")
            .register(meterRegistry);
    }

    public Flux<Product> findAll() {
        return productRepository.findAll();
    }

    public Mono<Product> findById(String id) {
        // Registrar la consulta y medir la latencia
        productosConsultadosCounter.increment();

        return busquedaTimer.record(
            productRepository.findById(id)
                .switchIfEmpty(Mono.defer(() -> {
                    // Incrementar contador de no encontrados
                    productosNoEncontradosCounter.increment();
                    return Mono.error(
                        new ProductNotFoundException("Producto no encontrado: " + id));
                }))
        );
    }

    public Mono<Product> save(Product product) {
        return productRepository.save(product)
            .doOnSuccess(saved -> {
                productosGuardadosCounter.increment();
                productosEnMemoria.incrementAndGet();
            });
    }
}
```

---

#### Paso 3.4 — Levantar Prometheus y Grafana con Docker Compose

**Instrucciones**:

1. Crea el archivo `docker/docker-compose-observability.yml` en la raíz del proyecto:

```yaml
version: '3.8'

services:
  prometheus:
    image: prom/prometheus:v2.51.0
    container_name: catalog-prometheus
    ports:
      - "9090:9090"
    volumes:
      - ./prometheus.yml:/etc/prometheus/prometheus.yml:ro
    networks:
      - observability-net
    restart: unless-stopped

  grafana:
    image: grafana/grafana:10.4.0
    container_name: catalog-grafana
    ports:
      - "3000:3000"
    environment:
      - GF_SECURITY_ADMIN_PASSWORD=admin123
      - GF_USERS_ALLOW_SIGN_UP=false
    volumes:
      - grafana-data:/var/lib/grafana
    networks:
      - observability-net
    depends_on:
      - prometheus
    restart: unless-stopped

volumes:
  grafana-data:

networks:
  observability-net:
    driver: bridge
```

2. Crea el archivo de configuración `docker/prometheus.yml`:

```yaml
global:
  scrape_interval: 15s
  evaluation_interval: 15s

scrape_configs:
  - job_name: 'catalog-service'
    # host.docker.internal permite que Prometheus acceda a la app en el host
    static_configs:
      - targets: ['host.docker.internal:8080']
    metrics_path: '/actuator/prometheus'
    scrape_interval: 10s
```

> **Nota para Linux**: En Linux, `host.docker.internal` puede no estar disponible. Usa la IP del host (`172.17.0.1` o la IP de tu interfaz `docker0`) en lugar de `host.docker.internal`.

3. Levanta el stack de observabilidad:

```bash
cd docker/
docker compose -f docker-compose-observability.yml up -d

# Verificar que los contenedores estén corriendo
docker compose -f docker-compose-observability.yml ps
```

4. Inicia la aplicación Spring Boot:

```bash
cd ..
mvn spring-boot:run &
```

5. Verifica que Prometheus está scrapeando la aplicación:

```bash
# Verificar endpoint de métricas
curl http://localhost:8080/actuator/prometheus | grep "catalog_products"

# Verificar health check
curl http://localhost:8080/actuator/health | python3 -m json.tool
```

6. Configura Grafana:
   - Abre `http://localhost:3000` en el navegador.
   - Credenciales: `admin` / `admin123`.
   - Ve a **Connections → Data Sources → Add data source → Prometheus**.
   - URL: `http://prometheus:9090` (dentro de la red Docker).
   - Clic en **Save & Test**.

7. Crea un dashboard básico con las métricas del catálogo:
   - Ve a **Dashboards → New → New Dashboard → Add visualization**.
   - Selecciona la fuente Prometheus.
   - Agrega las siguientes consultas PromQL:

```promql
# Tasa de consultas por segundo
rate(catalog_products_consulted_total[1m])

# Percentil 95 de latencia de búsqueda
histogram_quantile(0.95, rate(catalog_products_search_duration_seconds_bucket[5m]))

# Tasa de productos no encontrados
rate(catalog_products_not_found_total[1m])

# Total de productos guardados
catalog_products_saved_total
```

**Salida esperada**:
```bash
# Prometheus
curl http://localhost:9090/-/healthy
# Expected: Prometheus Server is Healthy.

# Métricas de la app
curl http://localhost:8080/actuator/prometheus | grep catalog_products_consulted
# Expected: catalog_products_consulted_total{...} 0.0
```

**Verificación**:
```bash
# Generar tráfico para ver métricas
for i in {1..10}; do
  curl -s http://localhost:8080/api/products/prod-001 > /dev/null
done

# Verificar que el counter incrementó
curl -s http://localhost:8080/actuator/prometheus | grep catalog_products_consulted_total
# Expected: catalog_products_consulted_total{...} 10.0
```

---

### Bloque 4: Identificación y Corrección de Antipatrones Reactivos (25 minutos)

**Objetivo del bloque**: Identificar, diagnosticar y corregir tres antipatrones reactivos críticos que aparecen con frecuencia en código de producción.

---

#### Paso 4.1 — Antipatrón 1: Bloqueo accidental en el event loop

**Objetivo**: Identificar el uso de `.block()` dentro de un controller WebFlux y corregirlo.

**Instrucciones**:

1. Examina el siguiente código con antipatrón (NO lo copies en producción tal cual):

```java
// ❌ ANTIPATRÓN: block() dentro de un controller WebFlux
// Esto bloquea el hilo del event loop, degradando el rendimiento
// y potencialmente causando deadlocks bajo carga alta.
@RestController
@RequestMapping("/api/products-bad")
public class ProductControllerBad {

    private final ProductService productService;

    public ProductControllerBad(ProductService productService) {
        this.productService = productService;
    }

    // ❌ MAL: block() en el event loop
    @GetMapping("/{id}")
    public Product getProduct(@PathVariable String id) {
        // block() espera el resultado bloqueando el hilo actual
        // En WebFlux, este hilo es del event loop → DEADLOCK potencial
        return productService.findById(id).block();  // ← NUNCA HACER ESTO
    }

    // ❌ MAL: block() con timeout (sigue siendo bloqueante)
    @GetMapping("/list")
    public List<Product> getAllProducts() {
        return productService.findAll()
            .collectList()
            .block(Duration.ofSeconds(5));  // ← NUNCA HACER ESTO
    }
}
```

2. Crea el controller corregido `ProductControllerGood.java`:

```java
// ✅ CORRECTO: Retornar Mono/Flux directamente
// Spring WebFlux sabe cómo manejar publishers como valores de retorno
@RestController
@RequestMapping("/api/products")
public class ProductController {

    private final ProductService productService;

    public ProductController(ProductService productService) {
        this.productService = productService;
    }

    // ✅ BIEN: Retornar Mono<Product> directamente
    @GetMapping("/{id}")
    public Mono<Product> getProduct(@PathVariable String id) {
        // Spring WebFlux se suscribe al Mono cuando el request llega
        // El hilo del event loop nunca se bloquea
        return productService.findById(id);
    }

    // ✅ BIEN: Retornar Flux<Product> directamente
    @GetMapping
    public Flux<Product> getAllProducts() {
        return productService.findAll();
    }

    // ✅ BIEN: Stream SSE — Flux con MediaType TEXT_EVENT_STREAM
    @GetMapping(value = "/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<Product> streamProducts() {
        return productService.findAll()
            .delayElements(Duration.ofMillis(500));
    }
}
```

3. Escribe un test que demuestre la diferencia de comportamiento:

```java
// src/test/java/com/ejemplo/catalog/antipattern/BlockingAntipatternTest.java
package com.ejemplo.catalog.antipattern;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Duration;

@DisplayName("Demostración de antipatrón: bloqueo en pipeline reactivo")
class BlockingAntipatternTest {

    @Test
    @DisplayName("Patrón correcto: encadenar operadores sin block()")
    void patronCorrecto_encadenarOperadoresSinBlock() {
        // ✅ CORRECTO: todo el procesamiento dentro del pipeline
        Mono<String> resultado = Mono.just("prod-001")
            .flatMap(id -> Mono.just("Laptop Pro: " + id))
            .map(String::toUpperCase);

        StepVerifier.create(resultado)
            .expectNext("LAPTOP PRO: PROD-001")
            .verifyComplete();
    }

    @Test
    @DisplayName("block() solo es aceptable en tests con justificación explícita")
    void blockSoloAceptableEnTests() {
        // ⚠️ ACEPTABLE SOLO EN TESTS: block() para obtener valor en test unitario
        // Preferir StepVerifier, pero block() es tolerable en tests no reactivos
        String resultado = Mono.just("valor-de-prueba").block(Duration.ofSeconds(1));
        assert "valor-de-prueba".equals(resultado);
    }
}
```

---

#### Paso 4.2 — Antipatrón 2: Uso incorrecto de Schedulers

**Objetivo**: Entender la diferencia entre `publishOn` y `subscribeOn`, y evitar bloquear el scheduler parallel.

**Instrucciones**:

1. Crea `src/test/java/com/ejemplo/catalog/antipattern/SchedulerAntipatternTest.java`:

```java
package com.ejemplo.catalog.antipattern;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import java.time.Duration;

@DisplayName("Antipatrón: uso incorrecto de Schedulers")
class SchedulerAntipatternTest {

    // Simula una operación bloqueante (ej: llamada a librería JDBC síncrona)
    private String operacionBloqueante(String id) throws InterruptedException {
        Thread.sleep(100); // Simula I/O bloqueante
        return "Resultado para: " + id;
    }

    @Test
    @DisplayName("❌ ANTIPATRÓN: operación bloqueante en scheduler parallel")
    void antipatron_operacionBloqueante_schedulerParallel() {
        // ❌ MAL: Schedulers.parallel() tiene pocos hilos (= núcleos CPU)
        // Una operación bloqueante aquí agotará el pool rápidamente
        // bajo carga concurrente.
        // Este test pasa, pero en producción causaría degradación severa.
        Mono<String> malo = Mono.fromCallable(() -> operacionBloqueante("001"))
            .subscribeOn(Schedulers.parallel()); // ← INCORRECTO para I/O bloqueante

        StepVerifier.create(malo)
            .expectNextMatches(r -> r.contains("001"))
            .verifyComplete();
        // Nota: el test pasa pero el patrón es incorrecto en producción
    }

    @Test
    @DisplayName("✅ CORRECTO: operación bloqueante en boundedElastic")
    void correcto_operacionBloqueante_boundedElastic() {
        // ✅ BIEN: Schedulers.boundedElastic() está diseñado para I/O bloqueante
        // Tiene un pool elástico que crece según demanda (con límite superior)
        Mono<String> correcto = Mono.fromCallable(() -> operacionBloqueante("001"))
            .subscribeOn(Schedulers.boundedElastic()); // ← CORRECTO para I/O bloqueante

        StepVerifier.create(correcto)
            .expectNextMatches(r -> r.contains("001"))
            .verifyComplete();
    }

    @Test
    @DisplayName("Diferencia entre subscribeOn y publishOn")
    void diferencia_subscribeOn_vs_publishOn() {
        // subscribeOn: afecta el hilo donde ocurre la SUSCRIPCIÓN (upstream)
        // publishOn: afecta el hilo donde se procesan los operadores DOWNSTREAM

        Mono<String> conPublishOn = Mono.just("dato")
            .map(s -> {
                // Este map corre en el hilo original (main/test)
                return s.toUpperCase();
            })
            .publishOn(Schedulers.boundedElastic())
            .map(s -> {
                // Este map corre en boundedElastic (después de publishOn)
                return s + "-procesado";
            });

        StepVerifier.create(conPublishOn)
            .expectNext("DATO-procesado")
            .verifyComplete();
    }
}
```

---

#### Paso 4.3 — Antipatrón 3: Propagación de contexto con ReactorContext

**Objetivo**: Usar `ReactorContext` para propagar un trace ID a través del pipeline sin pasar parámetros explícitos.

**Instrucciones**:

1. Crea `src/main/java/com/ejemplo/catalog/context/TraceContextExample.java`:

```java
package com.ejemplo.catalog.context;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.util.context.Context;

/**
 * Demuestra el uso de ReactorContext para propagar información
 * transversal (trace ID, usuario autenticado) sin pasar parámetros.
 *
 * ReactorContext es el equivalente reactivo a ThreadLocal.
 * Como los hilos cambian en pipelines reactivos, ThreadLocal NO funciona.
 */
public class TraceContextExample {

    private static final Logger log = LoggerFactory.getLogger(TraceContextExample.class);
    public static final String TRACE_ID_KEY = "traceId";
    public static final String USER_KEY = "authenticatedUser";

    /**
     * Servicio que lee el traceId del contexto reactivo para logging.
     * No recibe el traceId como parámetro → bajo acoplamiento.
     */
    public Mono<String> procesarConContexto(String productoId) {
        return Mono.deferContextual(ctx -> {
            // Leer el traceId del contexto reactivo
            String traceId = ctx.getOrDefault(TRACE_ID_KEY, "sin-trace");
            String usuario  = ctx.getOrDefault(USER_KEY, "anonimo");

            log.info("[traceId={}] [user={}] Procesando producto: {}",
                traceId, usuario, productoId);

            return Mono.just("Producto procesado: " + productoId)
                .doOnSuccess(result ->
                    log.info("[traceId={}] Resultado: {}", traceId, result));
        });
    }
}
```

2. Crea el test de contexto `src/test/java/com/ejemplo/catalog/context/ReactorContextTest.java`:

```java
package com.ejemplo.catalog.context;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import reactor.util.context.Context;

import static com.ejemplo.catalog.context.TraceContextExample.TRACE_ID_KEY;
import static com.ejemplo.catalog.context.TraceContextExample.USER_KEY;

@DisplayName("ReactorContext — Propagación de contexto transversal")
class ReactorContextTest {

    private final TraceContextExample service = new TraceContextExample();

    @Test
    @DisplayName("El contexto se propaga a través del pipeline sin parámetros explícitos")
    void contexto_sePropagaATravesDelPipeline() {
        // contextWrite se agrega al FINAL de la cadena (se lee de abajo hacia arriba)
        Mono<String> pipeline = service.procesarConContexto("prod-001")
            .contextWrite(Context.of(
                TRACE_ID_KEY, "trace-abc-123",
                USER_KEY, "usuario@ejemplo.com"
            ));

        StepVerifier.create(pipeline)
            .expectNextMatches(result -> result.contains("prod-001"))
            .verifyComplete();
    }

    @Test
    @DisplayName("❌ ANTIPATRÓN: usar ThreadLocal en pipeline reactivo")
    @DisplayName("ThreadLocal pierde el valor cuando el hilo cambia en el pipeline")
    void antipatron_threadLocal_pierdeContexto() {
        // ❌ MAL: ThreadLocal no funciona en pipelines reactivos
        // porque los operadores pueden ejecutarse en diferentes hilos
        ThreadLocal<String> traceLocal = new ThreadLocal<>();
        traceLocal.set("trace-xyz");

        Mono<String> conThreadLocal = Mono.just("dato")
            .publishOn(reactor.core.scheduler.Schedulers.boundedElastic())
            .map(s -> {
                // ❌ traceLocal.get() puede retornar null aquí
                // porque estamos en un hilo diferente al que hizo set()
                String trace = traceLocal.get(); // Puede ser null!
                return s + "-trace:" + trace;
            });

        // El test puede pasar o fallar dependiendo del scheduler
        // Este comportamiento NO es determinista → antipatrón
        StepVerifier.create(conThreadLocal)
            .expectNextMatches(result -> result.startsWith("dato-trace:"))
            .verifyComplete();

        // Limpiar ThreadLocal para evitar memory leaks
        traceLocal.remove();
    }

    @Test
    @DisplayName("✅ CORRECTO: ReactorContext mantiene el valor entre cambios de hilo")
    void correcto_reactorContext_mantieneValorEntreCambiosDeHilo() {
        Mono<String> conContexto = Mono.deferContextual(ctx ->
                Mono.just(ctx.getOrDefault(TRACE_ID_KEY, "sin-trace"))
            )
            .publishOn(reactor.core.scheduler.Schedulers.boundedElastic())
            .map(traceId -> "procesado-con-trace:" + traceId)
            // contextWrite siempre al final de la cadena
            .contextWrite(Context.of(TRACE_ID_KEY, "trace-estable-456"));

        StepVerifier.create(conContexto)
            .expectNext("procesado-con-trace:trace-estable-456")
            .verifyComplete();
    }
}
```

**Salida esperada**:
```
[INFO] Tests run: 3, Failures: 0, Errors: 0, Skipped: 0
[INFO] BUILD SUCCESS
```

**Verificación**:
```bash
mvn test -Dtest="*AntipatternTest,ReactorContextTest" -q
```

---

## Validación y Pruebas

Ejecuta la suite completa de tests del laboratorio:

```bash
# Ejecutar todos los tests del laboratorio
mvn test -q

# Verificar reporte de tests
cat target/surefire-reports/*.txt | grep -E "Tests run:|BUILD"
```

**Salida esperada completa**:
```
Tests run: 5, Failures: 0, Errors: 0, Skipped: 0  [ProductServiceTest]
Tests run: 4, Failures: 0, Errors: 0, Skipped: 0  [ProductServiceVirtualTimeTest]
Tests run: 5, Failures: 0, Errors: 0, Skipped: 0  [ProductControllerSliceTest]
Tests run: 4, Failures: 0, Errors: 0, Skipped: 0  [ProductIntegrationTest]
Tests run: 2, Failures: 0, Errors: 0, Skipped: 0  [BlockingAntipatternTest]
Tests run: 3, Failures: 0, Errors: 0, Skipped: 0  [SchedulerAntipatternTest]
Tests run: 3, Failures: 0, Errors: 0, Skipped: 0  [ReactorContextTest]

BUILD SUCCESS
Total tests run: 26, Failures: 0, Errors: 0
```

Verifica el stack de observabilidad:

```bash
# Prometheus scraping correctamente
curl -s http://localhost:9090/api/v1/targets | python3 -m json.tool | grep '"health"'
# Expected: "health": "up"

# Métricas de la aplicación disponibles en Prometheus
curl -s "http://localhost:9090/api/v1/query?query=catalog_products_consulted_total" \
  | python3 -m json.tool

# Health check de Actuator
curl -s http://localhost:8080/actuator/health | python3 -m json.tool
# Expected: "status": "UP" con componente "database": "UP"
```

Verifica la cobertura de código:

```bash
# Agregar plugin JaCoCo al pom.xml y generar reporte
mvn verify -q
open target/site/jacoco/index.html
# Expected: cobertura > 70% en clases de servicio
```

---

## Resolución de Problemas

### Problema 1: `StepVerifier` falla con "expectation failed (expected: onComplete(); actual: onError(...))"

**Síntomas**:
```
java.lang.AssertionError: expectation "expectComplete" failed
(expected: onComplete(); actual: onError(com.ejemplo.catalog.service.ProductNotFoundException: ...))
```

**Causa**: El mock del repositorio no está configurado para el ID exacto que usa el test, o el método del servicio tiene lógica de transformación de errores que no se anticipó. También puede ocurrir cuando `Mono.empty()` es transformado a `Mono.error()` por `switchIfEmpty` y el test espera `verifyComplete()` en lugar de `verifyError()`.

**Corrección**:
1. Verifica que el mock coincida exactamente con el argumento del test:
```java
// Verificar que el ID del mock coincide con el ID usado en el test
when(productRepository.findById("prod-001"))  // ← ID exacto
    .thenReturn(Mono.just(productoDePrueba));
```
2. Si el servicio usa `switchIfEmpty` para lanzar error, el test debe usar `verifyError()`:
```java
// Si findById transforma empty en error, usar verifyError:
StepVerifier.create(productService.findById("id-inexistente"))
    .expectError(ProductNotFoundException.class)
    .verify();  // NO verifyComplete()
```
3. Agrega logging en el pipeline para diagnosticar:
```java
productService.findById("id-inexistente")
    .log("DEBUG-TEST")  // Imprime todas las señales en consola
    // ... resto del pipeline
```

---

### Problema 2: Prometheus no puede scrapear la aplicación ("target down" en la UI)

**Síntomas**:
- En `http://localhost:9090/targets`, el target `catalog-service` aparece con estado `DOWN`.
- Error en Prometheus: `Get "http://host.docker.internal:8080/actuator/prometheus": dial tcp: no such host`.

**Causa**: En sistemas Linux, `host.docker.internal` no está disponible por defecto (es una característica de Docker Desktop en macOS/Windows). Prometheus dentro del contenedor no puede resolver el hostname del host.

**Corrección**:
1. Obtén la IP del gateway Docker en Linux:
```bash
# En Linux: obtener IP del bridge de Docker
docker network inspect bridge | grep Gateway
# Expected: "Gateway": "172.17.0.1"
```
2. Actualiza `docker/prometheus.yml` con la IP correcta:
```yaml
scrape_configs:
  - job_name: 'catalog-service'
    static_configs:
      # Reemplazar host.docker.internal con la IP del host en Linux
      - targets: ['172.17.0.1:8080']  # ← IP del gateway Docker en Linux
    metrics_path: '/actuator/prometheus'
```
3. Alternativamente, agrega `extra_hosts` al servicio Prometheus en `docker-compose-observability.yml`:
```yaml
services:
  prometheus:
    image: prom/prometheus:v2.51.0
    extra_hosts:
      - "host.docker.internal:host-gateway"  # Disponible en Docker Engine 20.10+
```
4. Reinicia el stack:
```bash
docker compose -f docker/docker-compose-observability.yml restart prometheus
```
5. Verifica la resolución:
```bash
docker exec catalog-prometheus wget -qO- http://host.docker.internal:8080/actuator/health
```

---

## Limpieza del Entorno

Ejecuta los siguientes comandos al finalizar el laboratorio:

```bash
# 1. Detener la aplicación Spring Boot (si está en background)
pkill -f "spring-boot:run" 2>/dev/null || true

# 2. Detener y eliminar contenedores de observabilidad
cd docker/
docker compose -f docker-compose-observability.yml down -v
# -v elimina también los volúmenes (datos de Grafana)

# 3. Verificar que los contenedores se eliminaron
docker ps | grep -E "prometheus|grafana"
# Expected: sin salida (contenedores eliminados)

# 4. Limpiar imágenes si es necesario (opcional)
docker image prune -f

# 5. Limpiar artefactos de build
cd ..
mvn clean -q

# 6. Verificar espacio liberado
docker system df
```

---

## Resumen

En este laboratorio aplicaste cuatro conjuntos de habilidades críticas para el desarrollo reactivo profesional:

| Bloque | Habilidad | Herramienta clave |
|--------|-----------|-------------------|
| 1 | Tests unitarios de pipelines reactivos | `StepVerifier`, `VirtualTimeScheduler` |
| 2 | Tests de integración HTTP reactivos | `WebTestClient`, `@WebFluxTest`, `@SpringBootTest` |
| 3 | Observabilidad y métricas de negocio | Actuator, Micrometer, Prometheus, Grafana |
| 4 | Diagnóstico y corrección de antipatrones | `ReactorContext`, `Schedulers`, análisis de código |

**Principios clave reforzados**:
- `StepVerifier` es la única forma correcta de probar publishers reactivos; nunca uses `.block()` en tests de pipelines.
- `withVirtualTime` permite probar operadores temporales en milisegundos de reloj real.
- `ReactiveHealthIndicator` es el reemplazo reactivo de `HealthIndicator` en aplicaciones WebFlux.
- `ThreadLocal` no funciona en pipelines reactivos; usa `ReactorContext` para propagar estado transversal.
- Las operaciones bloqueantes deben ejecutarse en `Schedulers.boundedElastic()`, nunca en `parallel()`.

### Recursos Adicionales

- [Documentación oficial de StepVerifier — Project Reactor](https://projectreactor.io/docs/core/release/reference/#testing)
- [WebTestClient — Spring Framework Reference](https://docs.spring.io/spring-framework/reference/testing/webtestclient.html)
- [Spring Boot Actuator — Referencia oficial](https://docs.spring.io/spring-boot/docs/current/reference/html/actuator.html)
- [Micrometer Concepts — micrometer.io](https://micrometer.io/docs/concepts)
- [Reactor Context — Project Reactor Reference](https://projectreactor.io/docs/core/release/reference/#context)
- [Prometheus Query Language (PromQL)](https://prometheus.io/docs/prometheus/latest/querying/basics/)

---
LAB_END---
