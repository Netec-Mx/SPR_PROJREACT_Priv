# Proyecto Demo end-to-end con pruebas y observabilidad

## Metadatos

| Campo        | Valor                                      |
|--------------|--------------------------------------------|
| Duración     | 30 minutos                                 |
| Complejidad  | Alta                                       |
| Nivel Bloom  | Crear                                      |
| Lab anterior | Lab 04 — Observabilidad con Actuator y Métricas |

---

## Descripción General

En este laboratorio integrador construirás desde cero una **Order Management API** completamente no bloqueante que consolida todos los componentes del curso: persistencia reactiva con R2DBC + PostgreSQL, endpoints WebFlux, integración externa con WebClient (con retry y fallback), manejo de errores RFC 7807, suite de tests con StepVerifier y WebTestClient, y métricas de negocio expuestas vía Actuator. Al finalizar, completarás un **checklist de 15 puntos** que constituye la evidencia principal de evaluación del curso.

> ⚠️ **ANTIPATRÓN CRÍTICO**: `block()`, `blockFirst()` y `blockLast()` están **prohibidos** dentro de controllers, servicios o cualquier pipeline reactivo en producción. Solo son aceptables en el método `main()` o en tests. Este principio se evalúa explícitamente en el checklist.

---

## Objetivos de Aprendizaje

Al finalizar este laboratorio serás capaz de:

- [ ] Diseñar e implementar una API reactiva completa integrando WebFlux, R2DBC, WebClient, Actuator y tests como evidencia de dominio del curso
- [ ] Aplicar manejo de errores robusto con timeouts, retries con backoff exponencial y fallback silencioso en integraciones externas
- [ ] Demostrar ausencia de bloqueo accidental mediante el uso correcto de schedulers y operadores reactivos en todos los niveles de la arquitectura
- [ ] Generar evidencias verificables de calidad: suite de tests con StepVerifier y WebTestClient, métricas de negocio expuestas y checklist completado

---

## Prerrequisitos

### Conocimiento

- Haber completado los Labs 01 al 04 del curso
- Dominio de operadores `Mono`/`Flux`: `flatMap`, `switchIfEmpty`, `onErrorMap`, `retryWhen`, `timeout`
- Experiencia con controllers WebFlux anotados, repositorios R2DBC y WebClient
- Capacidad de escribir tests con `StepVerifier` y `WebTestClient`

### Acceso y Herramientas

- JDK 21 LTS instalado y configurado en `JAVA_HOME`
- Maven 3.9.x disponible en el PATH
- Docker Desktop / Docker Engine 24.x en ejecución
- IntelliJ IDEA 2024.1+ (o editor equivalente)
- Conexión a Internet para descarga de dependencias Maven

---

## Entorno del Laboratorio

### Software requerido

| Componente          | Versión        | Rol en el lab                          |
|---------------------|----------------|----------------------------------------|
| Spring Boot         | 3.3.x          | Framework base                         |
| Project Reactor     | 3.6.x          | Motor reactivo                         |
| Spring Data R2DBC   | 3.3.x          | Persistencia reactiva                  |
| PostgreSQL (Docker) | 16-alpine      | Base de datos                          |
| Prometheus (Docker) | 2.51+          | Scraping de métricas                   |
| Java                | 21 LTS         | Lenguaje y runtime                     |
| Maven               | 3.9.x          | Gestión de dependencias                |

### Verificación rápida del entorno

```bash
# Verificar Java 21
java -version
# Expected: openjdk version "21.x.x"

# Verificar Maven
mvn -version
# Expected: Apache Maven 3.9.x

# Verificar Docker
docker info --format '{{.ServerVersion}}'
# Expected: 24.x o superior

# Verificar que el puerto 5432 esté libre
docker ps --filter "publish=5432"
```

---

## Pasos del Laboratorio

---

### Paso 1: Crear el proyecto Spring Boot con las dependencias correctas

**Objetivo**: Generar la estructura base del proyecto con todas las dependencias necesarias para la API de gestión de pedidos.

**Instrucciones**:

1. Crea el directorio del proyecto y genera el `pom.xml` con el siguiente contenido completo:

```bash
mkdir order-management-api && cd order-management-api
```

2. Crea el archivo `pom.xml` con el siguiente contenido:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0
         https://maven.apache.org/xsd/maven-4.0.0.xsd">
  <modelVersion>4.0.0</modelVersion>

  <parent>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-starter-parent</artifactId>
    <version>3.3.2</version>
    <relativePath/>
  </parent>

  <groupId>com.curso.reactor</groupId>
  <artifactId>order-management-api</artifactId>
  <version>1.0.0-SNAPSHOT</version>
  <name>order-management-api</name>

  <properties>
    <java.version>21</java.version>
  </properties>

  <dependencies>
    <!-- WebFlux -->
    <dependency>
      <groupId>org.springframework.boot</groupId>
      <artifactId>spring-boot-starter-webflux</artifactId>
    </dependency>

    <!-- R2DBC + PostgreSQL -->
    <dependency>
      <groupId>org.springframework.boot</groupId>
      <artifactId>spring-boot-starter-data-r2dbc</artifactId>
    </dependency>
    <dependency>
      <groupId>org.postgresql</groupId>
      <artifactId>r2dbc-postgresql</artifactId>
    </dependency>

    <!-- Validation -->
    <dependency>
      <groupId>org.springframework.boot</groupId>
      <artifactId>spring-boot-starter-validation</artifactId>
    </dependency>

    <!-- Actuator + Micrometer Prometheus -->
    <dependency>
      <groupId>org.springframework.boot</groupId>
      <artifactId>spring-boot-starter-actuator</artifactId>
    </dependency>
    <dependency>
      <groupId>io.micrometer</groupId>
      <artifactId>micrometer-registry-prometheus</artifactId>
    </dependency>

    <!-- Tests -->
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
    <dependency>
      <groupId>io.r2dbc</groupId>
      <artifactId>r2dbc-h2</artifactId>
      <scope>test</scope>
    </dependency>
  </dependencies>

  <build>
    <plugins>
      <plugin>
        <groupId>org.springframework.boot</groupId>
        <artifactId>spring-boot-maven-plugin</artifactId>
      </plugin>
    </plugins>
  </build>
</project>
```

3. Crea la estructura de directorios:

```bash
mkdir -p src/main/java/com/curso/reactor/orders/{domain,repository,service,controller,config,exception}
mkdir -p src/main/resources
mkdir -p src/test/java/com/curso/reactor/orders/{service,controller}
```

**Salida esperada**: El directorio `order-management-api/` con `pom.xml` y estructura de paquetes creada.

**Verificación**:
```bash
find src -type d | sort
# Debe mostrar todos los paquetes creados correctamente
```

---

### Paso 2: Configurar Docker Compose, base de datos y propiedades de la aplicación

**Objetivo**: Levantar PostgreSQL vía Docker y configurar R2DBC, Actuator y el esquema inicial.

**Instrucciones**:

1. Crea `docker-compose.yml` en la raíz del proyecto:

```yaml
version: '3.8'
services:
  postgres:
    image: postgres:16-alpine
    container_name: orders-postgres
    environment:
      POSTGRES_DB: ordersdb
      POSTGRES_USER: orders_user
      POSTGRES_PASSWORD: orders_pass
    ports:
      - "5432:5432"
    volumes:
      - postgres_data:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U orders_user -d ordersdb"]
      interval: 10s
      timeout: 5s
      retries: 5

  prometheus:
    image: prom/prometheus:v2.51.0
    container_name: orders-prometheus
    ports:
      - "9090:9090"
    volumes:
      - ./prometheus.yml:/etc/prometheus/prometheus.yml

volumes:
  postgres_data:
```

2. Crea `prometheus.yml` en la raíz:

```yaml
global:
  scrape_interval: 15s

scrape_configs:
  - job_name: 'order-management-api'
    static_configs:
      - targets: ['host.docker.internal:8080']
    metrics_path: '/actuator/prometheus'
```

3. Crea `src/main/resources/application.yml`:

```yaml
spring:
  application:
    name: order-management-api
  r2dbc:
    url: r2dbc:postgresql://localhost:5432/ordersdb
    username: orders_user
    password: orders_pass
  sql:
    init:
      mode: always
      schema-locations: classpath:schema.sql

server:
  port: 8080

management:
  endpoints:
    web:
      exposure:
        include: health,metrics,prometheus,info
  endpoint:
    health:
      show-details: always
  metrics:
    tags:
      application: ${spring.application.name}

notification:
  service:
    url: http://localhost:9999/notifications
    timeout-seconds: 2
    retry-attempts: 2
```

4. Crea `src/main/resources/schema.sql`:

```sql
CREATE TABLE IF NOT EXISTS customers (
    id        BIGSERIAL PRIMARY KEY,
    name      VARCHAR(200) NOT NULL,
    email     VARCHAR(200) NOT NULL UNIQUE,
    phone     VARCHAR(20)  NOT NULL
);

CREATE TABLE IF NOT EXISTS products (
    id        BIGSERIAL PRIMARY KEY,
    name      VARCHAR(200)   NOT NULL,
    price     NUMERIC(10, 2) NOT NULL CHECK (price > 0),
    stock     INTEGER        NOT NULL CHECK (stock >= 0)
);

CREATE TABLE IF NOT EXISTS orders (
    id          BIGSERIAL PRIMARY KEY,
    customer_id BIGINT      NOT NULL REFERENCES customers(id),
    created_at  TIMESTAMP   NOT NULL DEFAULT NOW(),
    status      VARCHAR(20) NOT NULL DEFAULT 'PENDING'
);

CREATE TABLE IF NOT EXISTS order_items (
    id         BIGSERIAL PRIMARY KEY,
    order_id   BIGINT  NOT NULL REFERENCES orders(id),
    product_id BIGINT  NOT NULL REFERENCES products(id),
    quantity   INTEGER NOT NULL CHECK (quantity >= 1)
);
```

5. Crea `src/test/resources/application-test.yml` para los tests con H2:

```yaml
spring:
  r2dbc:
    url: r2dbc:h2:mem:///testdb;DB_CLOSE_DELAY=-1
    username: sa
    password:
  sql:
    init:
      mode: always
      schema-locations: classpath:schema-test.sql
```

6. Crea `src/test/resources/schema-test.sql`:

```sql
CREATE TABLE IF NOT EXISTS customers (
    id        BIGINT AUTO_INCREMENT PRIMARY KEY,
    name      VARCHAR(200) NOT NULL,
    email     VARCHAR(200) NOT NULL,
    phone     VARCHAR(20)  NOT NULL
);

CREATE TABLE IF NOT EXISTS products (
    id    BIGINT AUTO_INCREMENT PRIMARY KEY,
    name  VARCHAR(200)   NOT NULL,
    price DECIMAL(10, 2) NOT NULL,
    stock INTEGER        NOT NULL
);

CREATE TABLE IF NOT EXISTS orders (
    id          BIGINT AUTO_INCREMENT PRIMARY KEY,
    customer_id BIGINT      NOT NULL,
    created_at  TIMESTAMP   NOT NULL,
    status      VARCHAR(20) NOT NULL
);

CREATE TABLE IF NOT EXISTS order_items (
    id         BIGINT AUTO_INCREMENT PRIMARY KEY,
    order_id   BIGINT  NOT NULL,
    product_id BIGINT  NOT NULL,
    quantity   INTEGER NOT NULL
);
```

7. Levanta los contenedores:

```bash
docker compose up -d
docker compose ps
# Espera a que postgres esté healthy
docker compose logs postgres --tail=5
```

**Salida esperada**:
```
NAME               STATUS
orders-postgres    running (healthy)
orders-prometheus  running
```

**Verificación**:
```bash
docker exec orders-postgres pg_isready -U orders_user -d ordersdb
# Expected: /var/run/postgresql:5432 - accepting connections
```

---

### Paso 3: Implementar el dominio, repositorios y capa de servicio

**Objetivo**: Crear las entidades R2DBC, los repositorios reactivos y la lógica de negocio con validaciones.

**Instrucciones**:

1. Crea las entidades de dominio en `src/main/java/com/curso/reactor/orders/domain/`:

```java
// Customer.java
package com.curso.reactor.orders.domain;

import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Table;
import jakarta.validation.constraints.*;

@Table("customers")
public record Customer(
    @Id Long id,
    @NotBlank @Size(max = 200) String name,
    @Email @NotBlank String email,
    @NotBlank @Pattern(regexp = "^\\+?[0-9]{7,15}$") String phone
) {}
```

```java
// Product.java
package com.curso.reactor.orders.domain;

import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Table;
import jakarta.validation.constraints.*;
import java.math.BigDecimal;

@Table("products")
public record Product(
    @Id Long id,
    @NotBlank @Size(max = 200) String name,
    @NotNull @DecimalMin("0.01") BigDecimal price,
    @NotNull @Min(0) Integer stock
) {}
```

```java
// Order.java
package com.curso.reactor.orders.domain;

import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Table;
import jakarta.validation.constraints.*;
import java.time.LocalDateTime;

@Table("orders")
public record Order(
    @Id Long id,
    @NotNull Long customerId,
    @NotNull LocalDateTime createdAt,
    @NotNull OrderStatus status
) {}
```

```java
// OrderStatus.java
package com.curso.reactor.orders.domain;
public enum OrderStatus { PENDING, CONFIRMED, CANCELLED }
```

```java
// OrderItem.java
package com.curso.reactor.orders.domain;

import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Table;
import jakarta.validation.constraints.*;

@Table("order_items")
public record OrderItem(
    @Id Long id,
    @NotNull Long orderId,
    @NotNull Long productId,
    @NotNull @Min(1) Integer quantity
) {}
```

2. Crea los DTOs de request/response en el paquete `domain`:

```java
// CreateOrderRequest.java
package com.curso.reactor.orders.domain;

import jakarta.validation.Valid;
import jakarta.validation.constraints.*;
import java.util.List;

public record CreateOrderRequest(
    @NotNull Long customerId,
    @NotEmpty @Valid List<OrderItemRequest> items
) {}
```

```java
// OrderItemRequest.java
package com.curso.reactor.orders.domain;

import jakarta.validation.constraints.*;

public record OrderItemRequest(
    @NotNull Long productId,
    @NotNull @Min(1) Integer quantity
) {}
```

3. Crea las excepciones en `exception/`:

```java
// CustomerNotFoundException.java
package com.curso.reactor.orders.exception;

public class CustomerNotFoundException extends RuntimeException {
    public CustomerNotFoundException(Long id) {
        super("Cliente no encontrado con ID: " + id);
    }
}
```

```java
// InsufficientStockException.java
package com.curso.reactor.orders.exception;

public class InsufficientStockException extends RuntimeException {
    public InsufficientStockException(Long productId, int available) {
        super("Stock insuficiente para producto ID " + productId
              + ". Disponible: " + available);
    }
}
```

4. Crea los repositorios en `repository/`:

```java
// CustomerRepository.java
package com.curso.reactor.orders.repository;

import com.curso.reactor.orders.domain.Customer;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import reactor.core.publisher.Mono;

public interface CustomerRepository
        extends ReactiveCrudRepository<Customer, Long> {
    Mono<Customer> findByEmail(String email);
}
```

```java
// ProductRepository.java
package com.curso.reactor.orders.repository;

import com.curso.reactor.orders.domain.Product;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;

public interface ProductRepository
        extends ReactiveCrudRepository<Product, Long> {}
```

```java
// OrderRepository.java
package com.curso.reactor.orders.repository;

import com.curso.reactor.orders.domain.Order;
import org.springframework.data.domain.Pageable;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import reactor.core.publisher.Flux;

public interface OrderRepository
        extends ReactiveCrudRepository<Order, Long> {
    Flux<Order> findByCustomerId(Long customerId, Pageable pageable);
    Flux<Order> findAllByOrderByCreatedAtDesc(Pageable pageable);
}
```

```java
// OrderItemRepository.java
package com.curso.reactor.orders.repository;

import com.curso.reactor.orders.domain.OrderItem;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import reactor.core.publisher.Flux;

public interface OrderItemRepository
        extends ReactiveCrudRepository<OrderItem, Long> {
    Flux<OrderItem> findByOrderId(Long orderId);
}
```

5. Crea el servicio principal en `service/OrderService.java`:

```java
package com.curso.reactor.orders.service;

import com.curso.reactor.orders.domain.*;
import com.curso.reactor.orders.exception.*;
import com.curso.reactor.orders.repository.*;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import java.time.LocalDateTime;

@Service
public class OrderService {

    private final OrderRepository orderRepository;
    private final OrderItemRepository itemRepository;
    private final CustomerRepository customerRepository;
    private final ProductRepository productRepository;
    private final NotificationService notificationService;
    private final Counter ordersCreatedCounter;

    public OrderService(OrderRepository orderRepository,
                        OrderItemRepository itemRepository,
                        CustomerRepository customerRepository,
                        ProductRepository productRepository,
                        NotificationService notificationService,
                        MeterRegistry meterRegistry) {
        this.orderRepository = orderRepository;
        this.itemRepository = itemRepository;
        this.customerRepository = customerRepository;
        this.productRepository = productRepository;
        this.notificationService = notificationService;
        // Métrica de negocio: contador de pedidos creados
        this.ordersCreatedCounter = Counter.builder("orders.created.total")
            .description("Total de pedidos creados exitosamente")
            .register(meterRegistry);
    }

    @Transactional
    public Mono<Order> createOrder(CreateOrderRequest request) {
        return customerRepository.findById(request.customerId())
            .switchIfEmpty(Mono.error(
                new CustomerNotFoundException(request.customerId())))
            .flatMap(customer -> validateStock(request.items()))
            .flatMap(__ -> {
                var order = new Order(null, request.customerId(),
                    LocalDateTime.now(), OrderStatus.PENDING);
                return orderRepository.save(order);
            })
            .flatMap(savedOrder -> saveItems(savedOrder, request.items())
                .then(Mono.just(savedOrder)))
            .doOnSuccess(order -> {
                ordersCreatedCounter.increment();
                // Notificación externa con fallback silencioso
                notificationService.notifyOrderCreated(order)
                    .subscribe();
            });
    }

    private Mono<Void> validateStock(java.util.List<OrderItemRequest> items) {
        return Flux.fromIterable(items)
            .flatMap(item -> productRepository.findById(item.productId())
                .switchIfEmpty(Mono.error(
                    new CustomerNotFoundException(item.productId())))
                .flatMap(product -> {
                    if (product.stock() < item.quantity()) {
                        return Mono.error(new InsufficientStockException(
                            product.id(), product.stock()));
                    }
                    return Mono.just(product);
                }))
            .then();
    }

    private Mono<Void> saveItems(Order order,
                                  java.util.List<OrderItemRequest> items) {
        return Flux.fromIterable(items)
            .map(item -> new OrderItem(null, order.id(),
                item.productId(), item.quantity()))
            .flatMap(itemRepository::save)
            .then();
    }

    public Mono<Order> findById(Long id) {
        return orderRepository.findById(id)
            .switchIfEmpty(Mono.error(
                new CustomerNotFoundException(id)));
    }

    public Flux<Order> findByCustomerPaged(Long customerId, int page, int size) {
        return orderRepository.findByCustomerId(
            customerId, PageRequest.of(page, size));
    }

    public Flux<Order> streamRecentOrders(int limit) {
        return orderRepository.findAllByOrderByCreatedAtDesc(
            PageRequest.of(0, limit));
    }
}
```

6. Crea el servicio de notificaciones en `service/NotificationService.java`:

```java
package com.curso.reactor.orders.service;

import com.curso.reactor.orders.domain.Order;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;
import java.time.Duration;

@Service
public class NotificationService {

    private static final Logger log =
        LoggerFactory.getLogger(NotificationService.class);

    private final WebClient webClient;
    private final int retryAttempts;
    private final Counter notificationFailureCounter;

    public NotificationService(
            WebClient.Builder builder,
            @Value("${notification.service.url}") String url,
            @Value("${notification.service.timeout-seconds:2}") int timeoutSec,
            @Value("${notification.service.retry-attempts:2}") int retryAttempts,
            MeterRegistry meterRegistry) {
        this.webClient = builder.baseUrl(url).build();
        this.retryAttempts = retryAttempts;
        this.notificationFailureCounter = Counter.builder("notifications.failures.total")
            .description("Total de fallos en notificaciones externas")
            .register(meterRegistry);
    }

    /**
     * Envía notificación con timeout 2s, retry 2 veces (backoff exponencial)
     * y fallback silencioso. NUNCA bloquea el event loop.
     */
    public Mono<Void> notifyOrderCreated(Order order) {
        return webClient.post()
            .uri("/order-created")
            .bodyValue(new OrderNotification(order.id(), order.customerId()))
            .retrieve()
            .bodyToMono(Void.class)
            .timeout(Duration.ofSeconds(2))
            .retryWhen(Retry.backoff(retryAttempts, Duration.ofMillis(200))
                .maxBackoff(Duration.ofSeconds(1))
                .doBeforeRetry(signal -> log.warn(
                    "Reintentando notificación, intento: {}",
                    signal.totalRetries() + 1)))
            .onErrorResume(ex -> {
                notificationFailureCounter.increment();
                log.warn("Notificación fallida para pedido {}. " +
                         "Fallback silencioso activado: {}",
                         order.id(), ex.getMessage());
                return Mono.empty(); // fallback silencioso
            });
    }

    record OrderNotification(Long orderId, Long customerId) {}
}
```

**Salida esperada**: Todos los archivos Java compilando sin errores.

**Verificación**:
```bash
mvn compile -q
echo "Exit code: $?"
# Expected: Exit code: 0
```

---

### Paso 4: Implementar los Controllers y el manejo de errores RFC 7807

**Objetivo**: Exponer los endpoints REST reactivos con manejo de errores estandarizado.

**Instrucciones**:

1. Crea el controller principal en `controller/OrderController.java`:

```java
package com.curso.reactor.orders.controller;

import com.curso.reactor.orders.domain.*;
import com.curso.reactor.orders.service.OrderService;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import java.time.Duration;

@RestController
@RequestMapping("/orders")
public class OrderController {

    private final OrderService orderService;

    public OrderController(OrderService orderService) {
        this.orderService = orderService;
    }

    /** POST /orders — Crear pedido con validación */
    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public Mono<Order> createOrder(@Valid @RequestBody CreateOrderRequest request) {
        return orderService.createOrder(request);
    }

    /** GET /orders/{id} — Consulta por ID */
    @GetMapping("/{id}")
    public Mono<Order> getOrderById(@PathVariable Long id) {
        return orderService.findById(id);
    }

    /**
     * GET /orders/stream — Stream SSE de pedidos recientes.
     * Emite los últimos 20 pedidos como Server-Sent Events con delay
     * de 100ms entre eventos para simular streaming real.
     */
    @GetMapping(value = "/stream",
                produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<Order> streamOrders() {
        return orderService.streamRecentOrders(20)
            .delayElements(Duration.ofMillis(100));
    }

    /** GET /customers/{id}/orders — Historial paginado */
    @GetMapping("/customers/{customerId}/orders")
    public Flux<Order> getCustomerOrders(
            @PathVariable Long customerId,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "10") int size) {
        return orderService.findByCustomerPaged(customerId, page, size);
    }
}
```

2. Crea el manejador global de errores en `config/GlobalErrorHandler.java`:

```java
package com.curso.reactor.orders.config;

import com.curso.reactor.orders.exception.*;
import org.springframework.http.*;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.bind.support.WebExchangeBindException;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;
import java.net.URI;

@RestControllerAdvice
public class GlobalErrorHandler {

    /** 404 — Recurso no encontrado */
    @ExceptionHandler(CustomerNotFoundException.class)
    @ResponseStatus(HttpStatus.NOT_FOUND)
    public Mono<ProblemDetail> handleNotFound(
            CustomerNotFoundException ex, ServerWebExchange exchange) {
        var problem = ProblemDetail.forStatusAndDetail(
            HttpStatus.NOT_FOUND, ex.getMessage());
        problem.setType(URI.create("https://api.orders.com/errors/not-found"));
        problem.setTitle("Recurso no encontrado");
        problem.setInstance(URI.create(
            exchange.getRequest().getPath().value()));
        return Mono.just(problem);
    }

    /** 422 — Stock insuficiente */
    @ExceptionHandler(InsufficientStockException.class)
    @ResponseStatus(HttpStatus.UNPROCESSABLE_ENTITY)
    public Mono<ProblemDetail> handleInsufficientStock(
            InsufficientStockException ex, ServerWebExchange exchange) {
        var problem = ProblemDetail.forStatusAndDetail(
            HttpStatus.UNPROCESSABLE_ENTITY, ex.getMessage());
        problem.setType(URI.create(
            "https://api.orders.com/errors/insufficient-stock"));
        problem.setTitle("Stock insuficiente");
        problem.setInstance(URI.create(
            exchange.getRequest().getPath().value()));
        return Mono.just(problem);
    }

    /** 400 — Errores de validación Bean Validation */
    @ExceptionHandler(WebExchangeBindException.class)
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public Mono<ProblemDetail> handleValidation(
            WebExchangeBindException ex, ServerWebExchange exchange) {
        var errors = ex.getBindingResult().getFieldErrors().stream()
            .map(fe -> fe.getField() + ": " + fe.getDefaultMessage())
            .toList();
        var problem = ProblemDetail.forStatusAndDetail(
            HttpStatus.BAD_REQUEST,
            "Errores de validación: " + errors);
        problem.setType(URI.create(
            "https://api.orders.com/errors/validation"));
        problem.setTitle("Error de validación");
        problem.setInstance(URI.create(
            exchange.getRequest().getPath().value()));
        return Mono.just(problem);
    }
}
```

3. Crea la clase principal de la aplicación en `src/main/java/com/curso/reactor/orders/OrderManagementApplication.java`:

```java
package com.curso.reactor.orders;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class OrderManagementApplication {
    public static void main(String[] args) {
        SpringApplication.run(OrderManagementApplication.class, args);
    }
}
```

**Salida esperada**: La aplicación compila y arranca correctamente.

**Verificación**:
```bash
mvn spring-boot:run &
sleep 10
curl -s http://localhost:8080/actuator/health | python3 -m json.tool
# Expected: {"status": "UP", ...}
# Detener la app
kill %1
```

---

### Paso 5: Escribir los tests con StepVerifier y WebTestClient

**Objetivo**: Implementar la suite mínima de tests requerida: 2 StepVerifier para la capa de servicio y 3 WebTestClient para los endpoints.

**Instrucciones**:

1. Crea el test de servicio en `src/test/java/com/curso/reactor/orders/service/OrderServiceTest.java`:

```java
package com.curso.reactor.orders.service;

import com.curso.reactor.orders.domain.*;
import com.curso.reactor.orders.exception.*;
import com.curso.reactor.orders.repository.*;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class OrderServiceTest {

    @Mock private OrderRepository orderRepository;
    @Mock private OrderItemRepository itemRepository;
    @Mock private CustomerRepository customerRepository;
    @Mock private ProductRepository productRepository;
    @Mock private NotificationService notificationService;

    private OrderService orderService;

    @BeforeEach
    void setUp() {
        orderService = new OrderService(
            orderRepository, itemRepository, customerRepository,
            productRepository, notificationService,
            new SimpleMeterRegistry()
        );
    }

    /**
     * StepVerifier Test 1:
     * Verifica que se lanza CustomerNotFoundException cuando el cliente no existe.
     */
    @Test
    @DisplayName("StepVerifier 1: createOrder lanza error cuando el cliente no existe")
    void createOrder_shouldErrorWhenCustomerNotFound() {
        var request = new CreateOrderRequest(99L,
            List.of(new OrderItemRequest(1L, 2)));

        when(customerRepository.findById(99L)).thenReturn(Mono.empty());

        StepVerifier.create(orderService.createOrder(request))
            .expectErrorMatches(ex ->
                ex instanceof CustomerNotFoundException &&
                ex.getMessage().contains("99"))
            .verify();
    }

    /**
     * StepVerifier Test 2:
     * Verifica que se lanza InsufficientStockException cuando no hay stock.
     */
    @Test
    @DisplayName("StepVerifier 2: createOrder lanza error cuando stock insuficiente")
    void createOrder_shouldErrorWhenInsufficientStock() {
        var customer = new Customer(1L, "Ana García",
            "ana@test.com", "+34600000001");
        var product = new Product(1L, "Laptop", BigDecimal.valueOf(999.99), 1);
        var request = new CreateOrderRequest(1L,
            List.of(new OrderItemRequest(1L, 5))); // pide 5, hay 1

        when(customerRepository.findById(1L)).thenReturn(Mono.just(customer));
        when(productRepository.findById(1L)).thenReturn(Mono.just(product));

        StepVerifier.create(orderService.createOrder(request))
            .expectErrorMatches(ex ->
                ex instanceof InsufficientStockException &&
                ex.getMessage().contains("producto ID 1"))
            .verify();
    }
}
```

2. Crea el test de integración de controllers en `src/test/java/com/curso/reactor/orders/controller/OrderControllerTest.java`:

```java
package com.curso.reactor.orders.controller;

import com.curso.reactor.orders.domain.*;
import com.curso.reactor.orders.exception.CustomerNotFoundException;
import com.curso.reactor.orders.service.OrderService;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.reactive.WebFluxTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.reactive.server.WebTestClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import java.time.LocalDateTime;
import java.util.List;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@WebFluxTest(OrderController.class)
class OrderControllerTest {

    @Autowired
    private WebTestClient webTestClient;

    @MockBean
    private OrderService orderService;

    /**
     * WebTestClient Test 1:
     * POST /orders retorna 201 CREATED con el pedido creado.
     */
    @Test
    @DisplayName("WebTestClient 1: POST /orders retorna 201 con pedido creado")
    void createOrder_shouldReturn201() {
        var order = new Order(1L, 1L, LocalDateTime.now(), OrderStatus.PENDING);
        var request = new CreateOrderRequest(1L,
            List.of(new OrderItemRequest(1L, 2)));

        when(orderService.createOrder(any(CreateOrderRequest.class)))
            .thenReturn(Mono.just(order));

        webTestClient.post().uri("/orders")
            .contentType(MediaType.APPLICATION_JSON)
            .bodyValue(request)
            .exchange()
            .expectStatus().isCreated()
            .expectBody(Order.class)
            .value(o -> {
                assert o.id().equals(1L);
                assert o.status() == OrderStatus.PENDING;
            });
    }

    /**
     * WebTestClient Test 2:
     * GET /orders/{id} retorna 404 con ProblemDetail cuando el pedido no existe.
     */
    @Test
    @DisplayName("WebTestClient 2: GET /orders/{id} retorna 404 cuando no existe")
    void getOrderById_shouldReturn404WhenNotFound() {
        when(orderService.findById(999L))
            .thenReturn(Mono.error(new CustomerNotFoundException(999L)));

        webTestClient.get().uri("/orders/999")
            .exchange()
            .expectStatus().isNotFound()
            .expectBody()
            .jsonPath("$.status").isEqualTo(404)
            .jsonPath("$.title").isEqualTo("Recurso no encontrado");
    }

    /**
     * WebTestClient Test 3:
     * GET /orders/stream retorna text/event-stream con elementos.
     */
    @Test
    @DisplayName("WebTestClient 3: GET /orders/stream retorna SSE con pedidos")
    void streamOrders_shouldReturnEventStream() {
        var orders = List.of(
            new Order(1L, 1L, LocalDateTime.now(), OrderStatus.CONFIRMED),
            new Order(2L, 2L, LocalDateTime.now(), OrderStatus.PENDING)
        );

        when(orderService.streamRecentOrders(20))
            .thenReturn(Flux.fromIterable(orders));

        webTestClient.get().uri("/orders/stream")
            .accept(MediaType.TEXT_EVENT_STREAM)
            .exchange()
            .expectStatus().isOk()
            .expectHeader().contentTypeCompatibleWith(
                MediaType.TEXT_EVENT_STREAM)
            .expectBodyList(Order.class)
            .hasSize(2);
    }
}
```

3. Ejecuta los tests y verifica que todos pasan:

```bash
mvn test -Dspring.profiles.active=test
```

**Salida esperada**:
```
[INFO] Tests run: 5, Failures: 0, Errors: 0, Skipped: 0
[INFO] BUILD SUCCESS
```

**Verificación**:
```bash
mvn test -Dspring.profiles.active=test | grep -E "Tests run:|BUILD"
# Expected: Tests run: 5, Failures: 0, Errors: 0, Skipped: 0
# Expected: BUILD SUCCESS
```

---

### Paso 6: Verificar métricas con Actuator y completar el checklist de calidad

**Objetivo**: Confirmar que Actuator expone `/health`, `/metrics` y la métrica de negocio, y completar el checklist de 15 puntos.

**Instrucciones**:

1. Inicia la aplicación con Docker Compose en ejecución:

```bash
# Asegúrate de que PostgreSQL está corriendo
docker compose up -d postgres

# Inicia la aplicación
mvn spring-boot:run &
sleep 15
```

2. Verifica los endpoints de Actuator:

```bash
# Health check
curl -s http://localhost:8080/actuator/health | python3 -m json.tool

# Listar todas las métricas disponibles
curl -s http://localhost:8080/actuator/metrics | python3 -m json.tool | grep -A2 "orders"

# Verificar métrica de negocio específica
curl -s "http://localhost:8080/actuator/metrics/orders.created.total" \
     | python3 -m json.tool

# Verificar endpoint Prometheus
curl -s http://localhost:8080/actuator/prometheus | grep "orders_created"
```

3. Inserta datos de prueba y crea un pedido para verificar la métrica:

```bash
# Insertar customer de prueba
docker exec -i orders-postgres psql -U orders_user -d ordersdb <<'EOF'
INSERT INTO customers (name, email, phone)
VALUES ('María López', 'maria@test.com', '+34600000001');
INSERT INTO products (name, price, stock)
VALUES ('Laptop Pro', 1299.99, 10);
EOF

# Crear un pedido via API
curl -s -X POST http://localhost:8080/orders \
  -H "Content-Type: application/json" \
  -d '{"customerId": 1, "items": [{"productId": 1, "quantity": 2}]}' \
  | python3 -m json.tool

# Verificar que el contador se incrementó
curl -s "http://localhost:8080/actuator/metrics/orders.created.total" \
     | python3 -m json.tool
```

4. Verifica el endpoint de stream SSE:

```bash
# Consume el stream durante 3 segundos
curl -s -N --max-time 3 \
  -H "Accept: text/event-stream" \
  http://localhost:8080/orders/stream
```

5. Completa el **Checklist de Calidad de 15 Puntos** — marca cada ítem verificado:

```markdown
## ✅ Checklist de Calidad — Order Management API

### Arquitectura Reactiva (5 puntos)
- [ ] 1. Todos los métodos de controller retornan Mono<T> o Flux<T>
- [ ] 2. No existe ningún block(), blockFirst() o blockLast() fuera de tests
- [ ] 3. La integración externa (NotificationService) usa WebClient no bloqueante
- [ ] 4. El retry con backoff exponencial está configurado correctamente (2 reintentos, 200ms base)
- [ ] 5. El fallback silencioso evita que fallos externos afecten la respuesta principal

### Persistencia y Datos (3 puntos)
- [ ] 6. Los repositorios extienden ReactiveCrudRepository
- [ ] 7. schema.sql crea todas las tablas correctamente al iniciar
- [ ] 8. La paginación en findByCustomerPaged usa Pageable de Spring Data

### Manejo de Errores (2 puntos)
- [ ] 9. CustomerNotFoundException retorna ProblemDetail con status 404
- [ ] 10. InsufficientStockException retorna ProblemDetail con status 422

### Tests (3 puntos)
- [ ] 11. StepVerifier Test 1 verifica error cuando cliente no existe
- [ ] 12. StepVerifier Test 2 verifica error cuando stock es insuficiente
- [ ] 13. Los 3 tests WebTestClient pasan (201, 404, SSE)

### Observabilidad (2 puntos)
- [ ] 14. /actuator/health retorna UP con detalles de la BD
- [ ] 15. La métrica orders.created.total se incrementa al crear pedidos
```

**Salida esperada**:

```json
// /actuator/health
{
  "status": "UP",
  "components": {
    "db": { "status": "UP" },
    "r2dbc": { "status": "UP" }
  }
}

// /actuator/metrics/orders.created.total (después de crear 1 pedido)
{
  "name": "orders.created.total",
  "measurements": [
    { "statistic": "COUNT", "value": 1.0 }
  ]
}
```

**Verificación final**:
```bash
# Todos los tests pasan
mvn test -Dspring.profiles.active=test | tail -5

# La app responde correctamente
curl -s http://localhost:8080/actuator/health \
  | python3 -c "import sys,json; d=json.load(sys.stdin); print('OK' if d['status']=='UP' else 'FAIL')"
# Expected: OK
```

---

## Validación y Pruebas

### Suite de tests completa

```bash
# Ejecutar todos los tests con reporte
mvn test -Dspring.profiles.active=test surefire-report:report
echo "=== Resultado ==="
mvn test -Dspring.profiles.active=test | grep -E "Tests run:|BUILD|FAILURE"
```

### Verificación de endpoints con curl

```bash
# 1. Verificar POST /orders con datos inválidos → 400
curl -s -X POST http://localhost:8080/orders \
  -H "Content-Type: application/json" \
  -d '{"customerId": null, "items": []}' \
  | python3 -m json.tool
# Expected: status 400, ProblemDetail con errores de validación

# 2. Verificar GET /orders/999 → 404
curl -s http://localhost:8080/orders/999 | python3 -m json.tool
# Expected: status 404, ProblemDetail

# 3. Verificar stream SSE
curl -s -N --max-time 2 \
  -H "Accept: text/event-stream" \
  http://localhost:8080/orders/stream
# Expected: data: {...} eventos JSON
```

### Criterios de aceptación

| Criterio | Comando de verificación | Resultado esperado |
|----------|------------------------|--------------------|
| Tests pasan | `mvn test` | 5 tests, 0 fallos |
| Health UP | `curl /actuator/health` | `"status":"UP"` |
| Métrica negocio | `curl /actuator/metrics/orders.created.total` | `"value": N` |
| 404 con ProblemDetail | `curl /orders/999` | `"status": 404` |
| SSE funciona | `curl -H "Accept: text/event-stream" /orders/stream` | `data: {...}` |
| Sin block() | `grep -r "\.block()" src/main/` | Sin resultados |

```bash
# Verificación crítica de antipatrón
grep -r "\.block()" src/main/java/ && echo "⚠️ ANTIPATRÓN DETECTADO" || echo "✅ Sin bloqueos"
grep -r "\.blockFirst()" src/main/java/ && echo "⚠️ ANTIPATRÓN DETECTADO" || echo "✅ Sin bloqueos"
```

---

## Solución de Problemas

### Problema 1: La aplicación falla al iniciar con "Connection refused" a PostgreSQL

**Síntoma**: La aplicación arranca pero inmediatamente lanza `io.r2dbc.postgresql.client.ReactorNettyClient` con `Connection refused` o `Unable to connect to localhost:5432`.

**Causa**: El contenedor Docker de PostgreSQL no está corriendo, no ha terminado su inicialización (healthcheck pendiente) o el puerto 5432 está siendo usado por otra instancia local de PostgreSQL.

**Solución**:
```bash
# 1. Verificar estado del contenedor
docker compose ps
# Si no está "healthy", esperar o revisar logs:
docker compose logs postgres --tail=20

# 2. Verificar que no haya otra instancia en el puerto
lsof -i :5432  # macOS/Linux
netstat -ano | findstr :5432  # Windows

# 3. Si hay conflicto de puerto, cambiar en docker-compose.yml:
# ports: - "5433:5432"
# Y actualizar application.yml:
# url: r2dbc:postgresql://localhost:5433/ordersdb

# 4. Reiniciar el stack completo
docker compose down -v && docker compose up -d
# Esperar healthcheck
docker compose ps --format "table {{.Name}}\t{{.Status}}"
```

---

### Problema 2: Los tests fallan con "No qualifying bean of type 'OrderService'"

**Síntoma**: Al ejecutar `mvn test`, los tests de `OrderControllerTest` fallan con `UnsatisfiedDependencyException` o `No qualifying bean of type 'OrderService' available`.

**Causa**: La anotación `@WebFluxTest` carga solo el slice de WebFlux (controllers y su infraestructura), pero **no** carga beans de servicio. Si falta `@MockBean` para `OrderService`, Spring no puede inyectarlo en el contexto de test.

**Solución**:
```java
// Verificar que OrderControllerTest tiene @MockBean para TODOS los beans
// que OrderController necesita:

@WebFluxTest(OrderController.class)
class OrderControllerTest {

    @Autowired
    private WebTestClient webTestClient;

    @MockBean  // ← OBLIGATORIO: no puede ser @Autowired aquí
    private OrderService orderService;

    // Si GlobalErrorHandler tiene dependencias adicionales,
    // también deben ser @MockBean
}
```

```bash
# Verificar que el perfil de test está activo
mvn test -Dspring.profiles.active=test -X 2>&1 | grep "Active profiles"
# Expected: Active profiles: test

# Si el error persiste, limpiar y recompilar
mvn clean test -Dspring.profiles.active=test
```

---

## Limpieza del Entorno

```bash
# 1. Detener la aplicación Spring Boot si está en ejecución
kill $(lsof -t -i:8080) 2>/dev/null || echo "App no estaba corriendo"

# 2. Detener y eliminar los contenedores Docker
docker compose down

# 3. Eliminar volúmenes de datos (opcional — borra datos de PostgreSQL)
docker compose down -v

# 4. Verificar que los puertos están libres
lsof -i :5432,8080,9090 2>/dev/null || echo "Puertos liberados correctamente"

# 5. Limpiar artefactos Maven (opcional)
mvn clean

# 6. Verificar que no quedan contenedores del lab
docker ps --filter "name=orders-" --format "table {{.Names}}\t{{.Status}}"
# Expected: tabla vacía
```

---

## Resumen

En este laboratorio has construido una **Order Management API** completamente no bloqueante que integra todos los componentes del curso como evidencia de dominio:

| Componente | Implementado | Verificado |
|------------|-------------|------------|
| Entidades R2DBC (`Customer`, `Product`, `Order`, `OrderItem`) | ✅ | ✅ |
| Repositorios reactivos con paginación | ✅ | ✅ |
| Servicio con validaciones de negocio reactivas | ✅ | ✅ |
| WebClient con timeout, retry backoff y fallback silencioso | ✅ | ✅ |
| Controllers WebFlux (POST, GET, GET stream, GET paginado) | ✅ | ✅ |
| Manejo de errores RFC 7807 (ProblemDetail) | ✅ | ✅ |
| 2 tests StepVerifier (capa de servicio) | ✅ | ✅ |
| 3 tests WebTestClient (endpoints principales) | ✅ | ✅ |
| Actuator: `/health` y `/metrics` | ✅ | ✅ |
| Métrica de negocio `orders.created.total` | ✅ | ✅ |
| Checklist de 15 puntos completado | ✅ | — |

### Principios reactivos demostrados

- **Sin bloqueos**: ningún `block()` en código de producción — verificado con `grep`
- **Propagación de errores**: `switchIfEmpty` → `Mono.error()` → `@ExceptionHandler`
- **Resiliencia externa**: `timeout` + `retryWhen(Retry.backoff(...))` + `onErrorResume`
- **Métricas de negocio**: `Counter` de Micrometer integrado en la capa de servicio

### Recursos de referencia

- [Spring WebFlux Reference Documentation](https://docs.spring.io/spring-framework/reference/web/webflux.html)
- [Spring Data R2DBC — Guía oficial](https://docs.spring.io/spring-data/relational/reference/r2dbc.html)
- [Project Reactor — Operadores de referencia](https://projectreactor.io/docs/core/release/reference/)
- [RFC 7807 — Problem Details for HTTP APIs](https://www.rfc-editor.org/rfc/rfc7807)
- [Micrometer — Métricas con Spring Boot](https://micrometer.io/docs/ref/spring/boot)
- [Reactor Test — StepVerifier](https://projectreactor.io/docs/core/release/reference/#testing)

---
*Lab 05-00-01 — Curso de Programación Reactiva con Spring WebFlux y Project Reactor*
