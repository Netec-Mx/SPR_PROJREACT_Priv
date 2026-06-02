---LAB_START---
LAB_ID: 03-00-01
---MARKDOWN---
# Integrar BD Reactiva + Endpoints de Consulta

## Metadatos

| Campo         | Valor                                      |
|---------------|--------------------------------------------|
| Duración      | 155 minutos                                |
| Complejidad   | Alta                                       |
| Nivel Bloom   | Aplicar (Apply)                            |
| Lab anterior  | Lab 02-00-01: WebFlux con controllers y WebClient básico |

---

## Visión General

En este laboratorio construirás una **API de catálogo de productos** que integra dos estrategias de persistencia reactiva intercambiables mediante perfiles de Spring: el perfil `relational` usa R2DBC con PostgreSQL, y el perfil `document` usa MongoDB Reactive. Ambas implementaciones exponen la misma interfaz de servicio, lo que te permitirá comparar las diferencias en configuración, mapeo y comportamiento. Adicionalmente, implementarás streaming de resultados con `Flux`, paginación reactiva, transacciones reactivas con rollback demostrable y un `WebClient` con retry exponencial y fallback que consume un servicio externo simulado con `MockWebServer`.

> ⚠️ **ANTIPATRÓN CRÍTICO**: Nunca uses `block()`, `blockFirst()` o `blockLast()` dentro de un controller WebFlux o en un pipeline reactivo en producción. Estos métodos solo son aceptables en el método `main()` o en tests. Este laboratorio refuerza ese principio en cada bloque.

---

## Objetivos de Aprendizaje

- [ ] Configurar Docker Compose con PostgreSQL y MongoDB, y activar cada motor mediante perfiles de Spring (`@Profile`).
- [ ] Implementar `ReactiveCrudRepository` y `ReactiveMongoRepository` con consultas derivadas y `@Query` personalizadas.
- [ ] Diseñar endpoints de streaming (`text/event-stream`) y paginación reactiva con `Pageable`.
- [ ] Aplicar transacciones reactivas con `@Transactional` en R2DBC y demostrar rollback automático.
- [ ] Integrar `WebClient` con timeout de 5 s, retry con backoff exponencial (3 intentos) y fallback al precio local.

---

## Prerrequisitos

### Conocimiento
- Haber completado **Lab 02-00-01**: WebFlux con controllers y WebClient básico.
- Conocimiento básico de SQL (SELECT, INSERT, UPDATE) y modelo relacional.
- Familiaridad con JSON y el concepto de colección/documento en MongoDB (no se requiere experiencia práctica previa).
- Comprensión de Spring Data JPA como base de comparación (repositorios, entidades).

### Acceso y Software
- JDK 21 LTS instalado y configurado (`JAVA_HOME`).
- Apache Maven 3.9.x.
- IntelliJ IDEA 2024.1+.
- Docker Desktop / Docker Engine 24.x funcional (`docker compose` disponible).
- Spring Boot 3.3.x (se configura en el `pom.xml`).
- Conexión a Internet para descargar dependencias de Maven Central e imágenes de Docker Hub.

---

## Entorno del Laboratorio

### Hardware mínimo recomendado

| Recurso    | Mínimo       | Recomendado  |
|------------|--------------|--------------|
| CPU        | 4 núcleos    | 8 núcleos    |
| RAM        | 8 GB libres  | 16 GB        |
| Disco      | 10 GB libres | 20 GB        |

### Software y versiones

| Herramienta             | Versión      |
|-------------------------|--------------|
| JDK                     | 21 LTS       |
| Maven                   | 3.9.x        |
| Spring Boot             | 3.3.x        |
| Docker Engine           | 24.x+        |
| PostgreSQL (Docker)     | 16-alpine    |
| MongoDB (Docker)        | 7.0          |
| OkHttp / MockWebServer  | 4.12.x       |

### Verificación del entorno antes de comenzar

Ejecuta los siguientes comandos y confirma que no hay errores:

```bash
# Verificar Java 21
java -version
# Esperado: openjdk version "21.x.x"

# Verificar Maven
mvn -version
# Esperado: Apache Maven 3.9.x

# Verificar Docker
docker info
docker compose version
# Esperado: Docker Engine 24.x, Compose v2.x

# Verificar conectividad a Docker Hub (descarga de prueba)
docker pull hello-world
```

---

## Instrucciones Paso a Paso

> **Nota de gestión del tiempo**: Se recomienda dividir este laboratorio en dos sesiones.
> - **Sesión A (80 min)**: Bloques 1, 2 y parte del Bloque 3.
> - **Sesión B (75 min)**: Bloque 3 completo, Bloques 4 y 5.

---

### Bloque 1 — Configuración del Proyecto y Docker Compose (25 min)

#### Objetivo
Crear el proyecto Spring Boot base con todas las dependencias necesarias y levantar PostgreSQL y MongoDB en Docker.

#### Instrucciones

**1.1 Crear el proyecto Maven**

Crea la estructura de directorios del proyecto:

```bash
mkdir lab03-catalog-api && cd lab03-catalog-api
mkdir -p src/main/java/com/curso/catalog/{config,controller,domain,repository,service,client}
mkdir -p src/main/resources
mkdir -p src/test/java/com/curso/catalog
```

**1.2 Crear el `pom.xml`**

Crea el archivo `pom.xml` en la raíz del proyecto con el siguiente contenido:

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
        <version>3.3.5</version>
        <relativePath/>
    </parent>

    <groupId>com.curso</groupId>
    <artifactId>lab03-catalog-api</artifactId>
    <version>1.0.0-SNAPSHOT</version>
    <name>lab03-catalog-api</name>

    <properties>
        <java.version>21</java.version>
    </properties>

    <dependencies>
        <!-- WebFlux: API reactiva no bloqueante -->
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-webflux</artifactId>
        </dependency>

        <!-- Validación de beans reactiva -->
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-validation</artifactId>
        </dependency>

        <!-- R2DBC: persistencia reactiva relacional -->
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-data-r2dbc</artifactId>
        </dependency>

        <!-- Driver R2DBC para PostgreSQL -->
        <dependency>
            <groupId>org.postgresql</groupId>
            <artifactId>r2dbc-postgresql</artifactId>
            <scope>runtime</scope>
        </dependency>

        <!-- Driver JDBC para PostgreSQL (solo para Flyway en startup) -->
        <dependency>
            <groupId>org.postgresql</groupId>
            <artifactId>postgresql</artifactId>
            <scope>runtime</scope>
        </dependency>

        <!-- MongoDB Reactive: persistencia reactiva documental -->
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-data-mongodb-reactive</artifactId>
        </dependency>

        <!-- Lombok: reducción de boilerplate -->
        <dependency>
            <groupId>org.projectlombok</groupId>
            <artifactId>lombok</artifactId>
            <optional>true</optional>
        </dependency>

        <!-- Tests reactivos -->
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

        <!-- MockWebServer para simular servicio externo de pricing -->
        <dependency>
            <groupId>com.squareup.okhttp3</groupId>
            <artifactId>mockwebserver</artifactId>
            <version>4.12.0</version>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>com.squareup.okhttp3</groupId>
            <artifactId>okhttp</artifactId>
            <version>4.12.0</version>
            <scope>test</scope>
        </dependency>
    </dependencies>

    <build>
        <plugins>
            <plugin>
                <groupId>org.springframework.boot</groupId>
                <artifactId>spring-boot-maven-plugin</artifactId>
                <configuration>
                    <excludes>
                        <exclude>
                            <groupId>org.projectlombok</groupId>
                            <artifactId>lombok</artifactId>
                        </exclude>
                    </excludes>
                </configuration>
            </plugin>
        </plugins>
    </build>
</project>
```

**1.3 Crear `docker-compose.yml`**

En la raíz del proyecto, crea el archivo `docker-compose.yml`:

```yaml
# docker-compose.yml — Infraestructura para Lab 03
version: "3.9"

services:
  postgres:
    image: postgres:16-alpine
    container_name: lab03-postgres
    environment:
      POSTGRES_DB: catalogdb
      POSTGRES_USER: catalog_user
      POSTGRES_PASSWORD: catalog_pass
    ports:
      - "5432:5432"
    volumes:
      - postgres_data:/var/lib/postgresql/data
      - ./src/main/resources/db/init.sql:/docker-entrypoint-initdb.d/init.sql
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U catalog_user -d catalogdb"]
      interval: 10s
      timeout: 5s
      retries: 5

  mongodb:
    image: mongo:7.0
    container_name: lab03-mongodb
    environment:
      MONGO_INITDB_DATABASE: catalogdb
    ports:
      - "27017:27017"
    volumes:
      - mongo_data:/data/db
    healthcheck:
      test: ["CMD", "mongosh", "--eval", "db.adminCommand('ping')"]
      interval: 10s
      timeout: 5s
      retries: 5

volumes:
  postgres_data:
  mongo_data:
```

**1.4 Crear el script SQL de inicialización**

Crea el directorio y el archivo de schema:

```bash
mkdir -p src/main/resources/db
```

Archivo `src/main/resources/db/init.sql`:

```sql
-- Schema inicial para el catálogo de productos en PostgreSQL
-- Este script se ejecuta automáticamente al iniciar el contenedor

CREATE TABLE IF NOT EXISTS products (
    id          BIGSERIAL PRIMARY KEY,
    name        VARCHAR(255) NOT NULL,
    description TEXT,
    category    VARCHAR(100) NOT NULL,
    price       NUMERIC(10, 2) NOT NULL,
    stock       INTEGER NOT NULL DEFAULT 0,
    active      BOOLEAN NOT NULL DEFAULT TRUE,
    created_at  TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Datos de prueba iniciales
INSERT INTO products (name, description, category, price, stock) VALUES
    ('Laptop Pro 15', 'Laptop de alto rendimiento con procesador i9', 'electronics', 1299.99, 50),
    ('Teclado Mecánico RGB', 'Teclado mecánico con switches Cherry MX', 'peripherals', 89.99, 200),
    ('Monitor 4K 27"', 'Monitor UHD con panel IPS y 144Hz', 'electronics', 449.99, 30),
    ('Mouse Ergonómico', 'Mouse inalámbrico con diseño ergonómico', 'peripherals', 49.99, 150),
    ('Auriculares Noise-Cancelling', 'Auriculares Bluetooth con cancelación activa de ruido', 'audio', 199.99, 75);
```

**1.5 Crear la clase principal de la aplicación**

Archivo `src/main/java/com/curso/catalog/CatalogApplication.java`:

```java
package com.curso.catalog;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class CatalogApplication {
    public static void main(String[] args) {
        SpringApplication.run(CatalogApplication.class, args);
    }
}
```

**1.6 Configurar `application.yml` con perfiles**

Archivo `src/main/resources/application.yml`:

```yaml
# Configuración base de la aplicación
spring:
  application:
    name: lab03-catalog-api
  # Perfil activo por defecto: relational (R2DBC + PostgreSQL)
  profiles:
    active: relational

server:
  port: 8080

logging:
  level:
    com.curso.catalog: DEBUG
    org.springframework.data.r2dbc: DEBUG
    org.springframework.data.mongodb: DEBUG

---
# Perfil 'relational': R2DBC + PostgreSQL
spring:
  config:
    activate:
      on-profile: relational
  r2dbc:
    url: r2dbc:postgresql://localhost:5432/catalogdb
    username: catalog_user
    password: catalog_pass
    pool:
      initial-size: 5
      max-size: 20
  # Deshabilitar autoconfiguración de MongoDB cuando usamos perfil relacional
  autoconfigure:
    exclude:
      - org.springframework.boot.autoconfigure.mongo.MongoReactiveAutoConfiguration
      - org.springframework.boot.autoconfigure.data.mongo.MongoReactiveRepositoriesAutoConfiguration

---
# Perfil 'document': MongoDB Reactive
spring:
  config:
    activate:
      on-profile: document
  data:
    mongodb:
      uri: mongodb://localhost:27017/catalogdb
  # Deshabilitar autoconfiguración de R2DBC cuando usamos perfil documental
  autoconfigure:
    exclude:
      - org.springframework.boot.autoconfigure.r2dbc.R2dbcAutoConfiguration
      - org.springframework.boot.autoconfigure.data.r2dbc.R2dbcRepositoriesAutoConfiguration
```

**1.7 Levantar la infraestructura Docker**

```bash
# Iniciar ambos contenedores en segundo plano
docker compose up -d

# Verificar que ambos están saludables (esperar ~30 segundos)
docker compose ps

# Verificar conectividad a PostgreSQL
docker exec lab03-postgres psql -U catalog_user -d catalogdb -c "SELECT COUNT(*) FROM products;"

# Verificar conectividad a MongoDB
docker exec lab03-mongodb mongosh --eval "db.adminCommand('ping')"
```

#### Salida esperada

```
NAME              IMAGE             STATUS
lab03-postgres    postgres:16-alpine   Up (healthy)
lab03-mongodb     mongo:7.0            Up (healthy)

# PostgreSQL debe mostrar:
 count
-------
     5
(1 row)

# MongoDB debe mostrar:
{ ok: 1 }
```

#### Verificación

```bash
# Confirmar que los puertos están escuchando
curl -s http://localhost:8080/actuator/health 2>/dev/null || echo "App no iniciada aún (normal en este punto)"
docker compose ps --format "table {{.Name}}\t{{.Status}}"
```

---

### Bloque 2 — Dominio, Repositorios Reactivos y Servicio (35 min)

#### Objetivo
Implementar las entidades de dominio, los repositorios reactivos para ambos motores y la interfaz de servicio común.

#### Instrucciones

**2.1 Crear el dominio compartido**

Archivo `src/main/java/com/curso/catalog/domain/Product.java`:

```java
package com.curso.catalog.domain;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * Clase de dominio compartida para ambos perfiles de persistencia.
 * R2DBC usará anotaciones @Table/@Column; MongoDB usará @Document/@Field.
 * Separamos las anotaciones en subclases para no mezclar dependencias.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Product {
    private String id;          // String para compatibilidad con ambos motores
    private String name;
    private String description;
    private String category;
    private BigDecimal price;
    private Integer stock;
    private Boolean active;
    private Instant createdAt;
}
```

**2.2 Entidad R2DBC**

Archivo `src/main/java/com/curso/catalog/domain/ProductEntity.java`:

```java
package com.curso.catalog.domain;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.CreatedDate;
import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Column;
import org.springframework.data.relational.core.mapping.Table;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * Entidad R2DBC mapeada a la tabla 'products' en PostgreSQL.
 * NOTA: Spring Data R2DBC NO usa JPA/Hibernate. No existe lazy loading
 * ni caché de primer nivel. Las relaciones deben manejarse con joins
 * manuales usando DatabaseClient o desnormalizando el modelo.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Table("products")
public class ProductEntity {

    @Id
    private Long id;

    @Column("name")
    private String name;

    @Column("description")
    private String description;

    @Column("category")
    private String category;

    @Column("price")
    private BigDecimal price;

    @Column("stock")
    private Integer stock;

    @Column("active")
    private Boolean active;

    @Column("created_at")
    @CreatedDate
    private Instant createdAt;

    // Mapeo de/hacia el dominio compartido
    public Product toDomain() {
        return Product.builder()
                .id(id != null ? id.toString() : null)
                .name(name)
                .description(description)
                .category(category)
                .price(price)
                .stock(stock)
                .active(active)
                .createdAt(createdAt)
                .build();
    }

    public static ProductEntity fromDomain(Product p) {
        return ProductEntity.builder()
                .id(p.getId() != null ? Long.valueOf(p.getId()) : null)
                .name(p.getName())
                .description(p.getDescription())
                .category(p.getCategory())
                .price(p.getPrice())
                .stock(p.getStock())
                .active(p.getActive() != null ? p.getActive() : true)
                .createdAt(p.getCreatedAt())
                .build();
    }
}
```

**2.3 Documento MongoDB**

Archivo `src/main/java/com/curso/catalog/domain/ProductDocument.java`:

```java
package com.curso.catalog.domain;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.CreatedDate;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Field;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * Documento MongoDB mapeado a la colección 'products'.
 * El esquema es flexible: campos adicionales pueden existir en la colección
 * sin romper esta clase.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Document(collection = "products")
public class ProductDocument {

    @Id
    private String id;

    @Field("name")
    private String name;

    @Field("description")
    private String description;

    @Indexed  // Índice para búsquedas frecuentes por categoría
    @Field("category")
    private String category;

    @Field("price")
    private BigDecimal price;

    @Field("stock")
    private Integer stock;

    @Field("active")
    private Boolean active;

    @CreatedDate
    @Field("created_at")
    private Instant createdAt;

    public Product toDomain() {
        return Product.builder()
                .id(id)
                .name(name)
                .description(description)
                .category(category)
                .price(price)
                .stock(stock)
                .active(active)
                .createdAt(createdAt)
                .build();
    }

    public static ProductDocument fromDomain(Product p) {
        return ProductDocument.builder()
                .id(p.getId())
                .name(p.getName())
                .description(p.getDescription())
                .category(p.getCategory())
                .price(p.getPrice())
                .stock(p.getStock())
                .active(p.getActive() != null ? p.getActive() : true)
                .createdAt(p.getCreatedAt())
                .build();
    }
}
```

**2.4 Repositorio R2DBC**

Archivo `src/main/java/com/curso/catalog/repository/R2dbcProductRepository.java`:

```java
package com.curso.catalog.repository;

import com.curso.catalog.domain.ProductEntity;
import org.springframework.context.annotation.Profile;
import org.springframework.data.domain.Pageable;
import org.springframework.data.r2dbc.repository.Query;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;

/**
 * Repositorio R2DBC activo solo con el perfil 'relational'.
 * ReactiveCrudRepository provee CRUD no bloqueante de forma automática.
 */
@Profile("relational")
public interface R2dbcProductRepository extends ReactiveCrudRepository<ProductEntity, Long> {

    // Consulta derivada: Spring Data genera el SQL automáticamente
    Flux<ProductEntity> findByCategory(String category);

    // Consulta derivada con paginación reactiva
    Flux<ProductEntity> findByCategory(String category, Pageable pageable);

    // Consulta derivada: rango de precios
    Flux<ProductEntity> findByPriceBetween(BigDecimal min, BigDecimal max);

    // Consulta personalizada con @Query (SQL nativo R2DBC)
    @Query("SELECT * FROM products WHERE active = true ORDER BY created_at DESC LIMIT :limit OFFSET :offset")
    Flux<ProductEntity> findActiveProducts(int limit, int offset);

    // Contar por categoría para paginación
    @Query("SELECT COUNT(*) FROM products WHERE category = :category")
    Mono<Long> countByCategory(String category);

    // Actualizar stock de forma atómica
    @Query("UPDATE products SET stock = stock - :quantity WHERE id = :id AND stock >= :quantity")
    Mono<Integer> decreaseStock(Long id, int quantity);
}
```

**2.5 Repositorio MongoDB**

Archivo `src/main/java/com/curso/catalog/repository/MongoProductRepository.java`:

```java
package com.curso.catalog.repository;

import com.curso.catalog.domain.ProductDocument;
import org.springframework.context.annotation.Profile;
import org.springframework.data.domain.Pageable;
import org.springframework.data.mongodb.repository.Query;
import org.springframework.data.mongodb.repository.ReactiveMongoRepository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;

/**
 * Repositorio MongoDB Reactive activo solo con el perfil 'document'.
 * ReactiveMongoRepository provee operaciones CRUD no bloqueantes.
 */
@Profile("document")
public interface MongoProductRepository extends ReactiveMongoRepository<ProductDocument, String> {

    // Consulta derivada equivalente al repositorio R2DBC
    Flux<ProductDocument> findByCategory(String category);

    // Consulta derivada con paginación
    Flux<ProductDocument> findByCategory(String category, Pageable pageable);

    // Consulta derivada: rango de precios
    Flux<ProductDocument> findByPriceBetween(BigDecimal min, BigDecimal max);

    // Consulta personalizada con @Query (MongoDB Query Language)
    @Query("{ 'active': true }")
    Flux<ProductDocument> findAllActive(Pageable pageable);

    // Contar por categoría
    Mono<Long> countByCategory(String category);
}
```

**2.6 Interfaz del servicio reactivo**

Archivo `src/main/java/com/curso/catalog/service/ProductService.java`:

```java
package com.curso.catalog.service;

import com.curso.catalog.domain.Product;
import org.springframework.data.domain.Pageable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;

/**
 * Interfaz común del servicio de catálogo.
 * Ambas implementaciones (R2DBC y MongoDB) exponen exactamente la misma API.
 * El código del controller no necesita saber qué motor de persistencia está activo.
 */
public interface ProductService {
    Mono<Product> findById(String id);
    Flux<Product> findAll(Pageable pageable);
    Flux<Product> findByCategory(String category, Pageable pageable);
    Flux<Product> findByPriceRange(BigDecimal min, BigDecimal max);
    Flux<Product> streamAllProducts();   // Para el endpoint de streaming
    Mono<Product> save(Product product);
    Mono<Product> update(String id, Product product);
    Mono<Void> delete(String id);
    Mono<Product> transferInventory(String sourceId, String targetId, int quantity); // Para transacciones
}
```

**2.7 Implementación del servicio R2DBC**

Archivo `src/main/java/com/curso/catalog/service/R2dbcProductService.java`:

```java
package com.curso.catalog.service;

import com.curso.catalog.domain.Product;
import com.curso.catalog.domain.ProductEntity;
import com.curso.catalog.repository.R2dbcProductRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Profile;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.time.Duration;

@Slf4j
@Service
@Profile("relational")
@RequiredArgsConstructor
public class R2dbcProductService implements ProductService {

    private final R2dbcProductRepository repository;

    @Override
    public Mono<Product> findById(String id) {
        return repository.findById(Long.valueOf(id))
                .map(ProductEntity::toDomain)
                .doOnNext(p -> log.debug("[R2DBC] Producto encontrado: {}", p.getId()));
    }

    @Override
    public Flux<Product> findAll(Pageable pageable) {
        return repository.findActiveProducts(pageable.getPageSize(),
                        (int) pageable.getOffset())
                .map(ProductEntity::toDomain);
    }

    @Override
    public Flux<Product> findByCategory(String category, Pageable pageable) {
        return repository.findByCategory(category, pageable)
                .map(ProductEntity::toDomain);
    }

    @Override
    public Flux<Product> findByPriceRange(BigDecimal min, BigDecimal max) {
        return repository.findByPriceBetween(min, max)
                .map(ProductEntity::toDomain);
    }

    @Override
    public Flux<Product> streamAllProducts() {
        // delayElements simula streaming real con backpressure
        return repository.findAll()
                .map(ProductEntity::toDomain)
                .delayElements(Duration.ofMillis(500))
                .doOnNext(p -> log.debug("[R2DBC] Streaming producto: {}", p.getName()));
    }

    @Override
    @Transactional  // Transacción reactiva R2DBC
    public Mono<Product> save(Product product) {
        ProductEntity entity = ProductEntity.fromDomain(product);
        return repository.save(entity)
                .map(ProductEntity::toDomain)
                .doOnSuccess(p -> log.info("[R2DBC] Producto guardado con id: {}", p.getId()));
    }

    @Override
    @Transactional
    public Mono<Product> update(String id, Product product) {
        return repository.findById(Long.valueOf(id))
                .switchIfEmpty(Mono.error(new RuntimeException("Producto no encontrado: " + id)))
                .flatMap(existing -> {
                    existing.setName(product.getName());
                    existing.setDescription(product.getDescription());
                    existing.setCategory(product.getCategory());
                    existing.setPrice(product.getPrice());
                    existing.setStock(product.getStock());
                    return repository.save(existing);
                })
                .map(ProductEntity::toDomain);
    }

    @Override
    @Transactional
    public Mono<Void> delete(String id) {
        return repository.deleteById(Long.valueOf(id));
    }

    /**
     * Transferencia de inventario con transacción reactiva.
     * Si el stock del origen es insuficiente, la transacción hace ROLLBACK automático.
     * NOTA: @Transactional en R2DBC requiere que el contexto reactivo propague
     * la transacción correctamente — no uses block() aquí.
     */
    @Override
    @Transactional
    public Mono<Product> transferInventory(String sourceId, String targetId, int quantity) {
        return repository.decreaseStock(Long.valueOf(sourceId), quantity)
                .flatMap(rowsUpdated -> {
                    if (rowsUpdated == 0) {
                        // Lanzar error provoca ROLLBACK de la transacción
                        return Mono.error(new RuntimeException(
                                "Stock insuficiente en producto " + sourceId));
                    }
                    // Incrementar stock en el destino
                    return repository.findById(Long.valueOf(targetId))
                            .flatMap(target -> {
                                target.setStock(target.getStock() + quantity);
                                return repository.save(target);
                            });
                })
                .map(ProductEntity::toDomain);
    }
}
```

**2.8 Implementación del servicio MongoDB**

Archivo `src/main/java/com/curso/catalog/service/MongoProductService.java`:

```java
package com.curso.catalog.service;

import com.curso.catalog.domain.Product;
import com.curso.catalog.domain.ProductDocument;
import com.curso.catalog.repository.MongoProductRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Profile;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.time.Duration;

@Slf4j
@Service
@Profile("document")
@RequiredArgsConstructor
public class MongoProductService implements ProductService {

    private final MongoProductRepository repository;

    @Override
    public Mono<Product> findById(String id) {
        return repository.findById(id)
                .map(ProductDocument::toDomain)
                .doOnNext(p -> log.debug("[MongoDB] Producto encontrado: {}", p.getId()));
    }

    @Override
    public Flux<Product> findAll(Pageable pageable) {
        return repository.findAllActive(pageable)
                .map(ProductDocument::toDomain);
    }

    @Override
    public Flux<Product> findByCategory(String category, Pageable pageable) {
        return repository.findByCategory(category, pageable)
                .map(ProductDocument::toDomain);
    }

    @Override
    public Flux<Product> findByPriceRange(BigDecimal min, BigDecimal max) {
        return repository.findByPriceBetween(min, max)
                .map(ProductDocument::toDomain);
    }

    @Override
    public Flux<Product> streamAllProducts() {
        return repository.findAll()
                .map(ProductDocument::toDomain)
                .delayElements(Duration.ofMillis(500))
                .doOnNext(p -> log.debug("[MongoDB] Streaming producto: {}", p.getName()));
    }

    @Override
    public Mono<Product> save(Product product) {
        return repository.save(ProductDocument.fromDomain(product))
                .map(ProductDocument::toDomain)
                .doOnSuccess(p -> log.info("[MongoDB] Producto guardado con id: {}", p.getId()));
    }

    @Override
    public Mono<Product> update(String id, Product product) {
        return repository.findById(id)
                .switchIfEmpty(Mono.error(new RuntimeException("Producto no encontrado: " + id)))
                .flatMap(existing -> {
                    existing.setName(product.getName());
                    existing.setDescription(product.getDescription());
                    existing.setCategory(product.getCategory());
                    existing.setPrice(product.getPrice());
                    existing.setStock(product.getStock());
                    return repository.save(existing);
                })
                .map(ProductDocument::toDomain);
    }

    @Override
    public Mono<Void> delete(String id) {
        return repository.deleteById(id);
    }

    /**
     * En MongoDB, las transacciones multi-documento requieren un Replica Set.
     * Para desarrollo, simulamos la atomicidad con operaciones secuenciales.
     * En producción con Replica Set, @Transactional funciona correctamente.
     */
    @Override
    @Transactional
    public Mono<Product> transferInventory(String sourceId, String targetId, int quantity) {
        return repository.findById(sourceId)
                .switchIfEmpty(Mono.error(new RuntimeException("Origen no encontrado: " + sourceId)))
                .flatMap(source -> {
                    if (source.getStock() < quantity) {
                        return Mono.error(new RuntimeException("Stock insuficiente: " + source.getStock()));
                    }
                    source.setStock(source.getStock() - quantity);
                    return repository.save(source);
                })
                .flatMap(savedSource -> repository.findById(targetId)
                        .flatMap(target -> {
                            target.setStock(target.getStock() + quantity);
                            return repository.save(target);
                        }))
                .map(ProductDocument::toDomain);
    }
}
```

#### Verificación del Bloque 2

```bash
# Compilar el proyecto para detectar errores de compilación
mvn compile -q

# Salida esperada: BUILD SUCCESS (sin errores)
```

---

### Bloque 3 — Controller REST: Paginación y Streaming (30 min)

#### Objetivo
Implementar el controller con endpoints de paginación reactiva y streaming SSE (`text/event-stream`).

#### Instrucciones

**3.1 Crear el ProductController**

Archivo `src/main/java/com/curso/catalog/controller/ProductController.java`:

```java
package com.curso.catalog.controller;

import com.curso.catalog.domain.Product;
import com.curso.catalog.service.ProductService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.PageRequest;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;

@Slf4j
@RestController
@RequestMapping("/products")
@RequiredArgsConstructor
public class ProductController {

    private final ProductService productService;

    /**
     * GET /products?page=0&size=10
     * Paginación reactiva: devuelve un Flux paginado de productos activos.
     */
    @GetMapping(produces = MediaType.APPLICATION_JSON_VALUE)
    public Flux<Product> getProducts(
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "10") int size) {
        log.debug("GET /products?page={}&size={}", page, size);
        return productService.findAll(PageRequest.of(page, size));
    }

    /**
     * GET /products/stream
     * Streaming de productos como Server-Sent Events (SSE).
     * El cliente recibe cada producto a medida que se emite, con backpressure.
     * NOTA: MediaType.TEXT_EVENT_STREAM_VALUE es esencial para SSE.
     */
    @GetMapping(value = "/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<Product> streamProducts() {
        log.debug("GET /products/stream — iniciando stream SSE");
        return productService.streamAllProducts();
    }

    /**
     * GET /products/{id}
     * Búsqueda por ID con manejo de not-found reactivo.
     */
    @GetMapping("/{id}")
    public Mono<ResponseEntity<Product>> getById(@PathVariable String id) {
        return productService.findById(id)
                .map(ResponseEntity::ok)
                .defaultIfEmpty(ResponseEntity.notFound().build());
    }

    /**
     * GET /products/category/{category}?page=0&size=10
     * Filtrado por categoría con paginación.
     */
    @GetMapping("/category/{category}")
    public Flux<Product> getByCategory(
            @PathVariable String category,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "10") int size) {
        return productService.findByCategory(category, PageRequest.of(page, size));
    }

    /**
     * GET /products/price-range?min=10&max=500
     * Filtrado por rango de precios.
     */
    @GetMapping("/price-range")
    public Flux<Product> getByPriceRange(
            @RequestParam BigDecimal min,
            @RequestParam BigDecimal max) {
        return productService.findByPriceRange(min, max);
    }

    /**
     * POST /products
     * Crear un nuevo producto.
     */
    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public Mono<Product> createProduct(@Valid @RequestBody Product product) {
        return productService.save(product);
    }

    /**
     * PUT /products/{id}
     * Actualizar un producto existente.
     */
    @PutMapping("/{id}")
    public Mono<ResponseEntity<Product>> updateProduct(
            @PathVariable String id,
            @Valid @RequestBody Product product) {
        return productService.update(id, product)
                .map(ResponseEntity::ok)
                .onErrorReturn(RuntimeException.class,
                        ResponseEntity.notFound().build());
    }

    /**
     * DELETE /products/{id}
     */
    @DeleteMapping("/{id}")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public Mono<Void> deleteProduct(@PathVariable String id) {
        return productService.delete(id);
    }

    /**
     * POST /products/transfer-inventory
     * Transferencia de inventario con transacción reactiva.
     * Body: { "sourceId": "1", "targetId": "2", "quantity": 5 }
     */
    @PostMapping("/transfer-inventory")
    public Mono<ResponseEntity<Product>> transferInventory(
            @RequestBody TransferRequest request) {
        return productService.transferInventory(
                        request.sourceId(), request.targetId(), request.quantity())
                .map(ResponseEntity::ok)
                .onErrorResume(RuntimeException.class, ex ->
                        Mono.just(ResponseEntity.badRequest().<Product>build()));
    }

    // Record para el request de transferencia
    public record TransferRequest(String sourceId, String targetId, int quantity) {}
}
```

**3.2 Iniciar la aplicación con perfil `relational`**

```bash
# Iniciar con perfil relacional (R2DBC + PostgreSQL)
mvn spring-boot:run -Dspring-boot.run.profiles=relational

# En otra terminal, verificar los endpoints
curl -s http://localhost:8080/products | python3 -m json.tool

# Probar paginación
curl -s "http://localhost:8080/products?page=0&size=3" | python3 -m json.tool

# Probar filtrado por categoría
curl -s "http://localhost:8080/products/category/electronics" | python3 -m json.tool
```

**3.3 Probar el endpoint de streaming**

```bash
# Probar streaming SSE (Ctrl+C para detener)
# Cada producto aparece con un delay de 500ms
curl -N -H "Accept: text/event-stream" http://localhost:8080/products/stream
```

#### Salida esperada del streaming

```
data:{"id":"1","name":"Laptop Pro 15","category":"electronics","price":1299.99,...}

data:{"id":"2","name":"Teclado Mecánico RGB","category":"peripherals","price":89.99,...}

data:{"id":"3","name":"Monitor 4K 27\"","category":"electronics","price":449.99,...}
```

**3.4 Cambiar al perfil `document` y verificar**

```bash
# Detener la aplicación (Ctrl+C) y reiniciar con perfil document
mvn spring-boot:run -Dspring-boot.run.profiles=document

# Primero insertar datos en MongoDB (el init.sql no aplica a Mongo)
curl -s -X POST http://localhost:8080/products \
  -H "Content-Type: application/json" \
  -d '{"name":"Laptop Pro 15","description":"Laptop de alto rendimiento","category":"electronics","price":1299.99,"stock":50,"active":true}'

# Verificar que el endpoint funciona igual
curl -s http://localhost:8080/products | python3 -m json.tool
```

#### Verificación del Bloque 3

El mismo `curl` debe funcionar con ambos perfiles. La respuesta JSON debe ser idéntica en estructura, demostrando que la interfaz del servicio abstrae correctamente el motor de persistencia.

---

### Bloque 4 — Transacciones Reactivas y Rollback (25 min)

#### Objetivo
Demostrar el comportamiento de `@Transactional` en R2DBC con un rollback automático por error de negocio.

#### Instrucciones

**4.1 Preparar datos para la prueba de transacción**

```bash
# Asegúrate de estar ejecutando con perfil 'relational'
# Verificar el stock actual de los productos 1 y 2
curl -s http://localhost:8080/products/1 | python3 -m json.tool
curl -s http://localhost:8080/products/2 | python3 -m json.tool
```

**4.2 Prueba de transferencia exitosa**

```bash
# Transferir 10 unidades del producto 1 al producto 2
curl -s -X POST http://localhost:8080/products/transfer-inventory \
  -H "Content-Type: application/json" \
  -d '{"sourceId":"1","targetId":"2","quantity":10}' | python3 -m json.tool

# Verificar que el stock cambió correctamente
curl -s http://localhost:8080/products/1 | python3 -m json.tool  # stock debe ser 40
curl -s http://localhost:8080/products/2 | python3 -m json.tool  # stock debe ser 210
```

**4.3 Prueba de rollback por stock insuficiente**

```bash
# Intentar transferir más unidades de las disponibles (debería fallar)
curl -s -X POST http://localhost:8080/products/transfer-inventory \
  -H "Content-Type: application/json" \
  -d '{"sourceId":"1","targetId":"2","quantity":9999}' \
  -w "\nHTTP Status: %{http_code}\n"

# Verificar que el stock NO cambió (rollback efectivo)
curl -s http://localhost:8080/products/1 | python3 -m json.tool  # stock debe seguir siendo 40
curl -s http://localhost:8080/products/2 | python3 -m json.tool  # stock debe seguir siendo 210
```

#### Salida esperada del rollback

```
HTTP Status: 400

# Los stocks permanecen sin cambios, confirmando el rollback
```

**4.4 Observar el log de la transacción**

En la consola de la aplicación deberías ver:

```
DEBUG o.s.r2dbc.core.DefaultDatabaseClient - Executing SQL statement [UPDATE products SET stock = stock - :quantity ...]
DEBUG o.s.t.r.TransactionSynchronizationManager - Rolling back R2DBC transaction
```

#### Verificación del Bloque 4

```bash
# Script de verificación completo
echo "=== Verificación de Rollback ==="
STOCK_1=$(curl -s http://localhost:8080/products/1 | python3 -c "import sys,json; print(json.load(sys.stdin)['stock'])")
echo "Stock producto 1: $STOCK_1 (esperado: 40)"
```

---

### Bloque 5 — WebClient con Retry Exponencial y Fallback (40 min)

#### Objetivo
Integrar un `WebClient` que consulta un servicio de pricing externo simulado con `MockWebServer`, implementando timeout de 5 s, retry con backoff exponencial (3 intentos) y fallback al precio local.

#### Instrucciones

**5.1 Crear el cliente del servicio de pricing**

Archivo `src/main/java/com/curso/catalog/client/PricingClient.java`:

```java
package com.curso.catalog.client;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import java.math.BigDecimal;
import java.time.Duration;

/**
 * Cliente reactivo para el servicio externo de pricing.
 * Implementa:
 *   - Timeout de 5 segundos por petición
 *   - Retry con backoff exponencial: 3 intentos, delay inicial 1s, máximo 8s
 *   - Fallback al precio almacenado localmente si el servicio externo falla
 */
@Slf4j
@Component
public class PricingClient {

    private final WebClient webClient;

    public PricingClient(
            WebClient.Builder webClientBuilder,
            @Value("${pricing.service.url:http://localhost:8090}") String pricingServiceUrl) {
        this.webClient = webClientBuilder
                .baseUrl(pricingServiceUrl)
                .build();
    }

    /**
     * Consulta el precio actualizado de un producto en el servicio externo.
     *
     * @param productId  ID del producto
     * @param localPrice Precio local como fallback si el servicio falla
     * @return Mono con el precio actualizado o el precio local en caso de fallo
     */
    public Mono<BigDecimal> getUpdatedPrice(String productId, BigDecimal localPrice) {
        return webClient.get()
                .uri("/pricing/{productId}", productId)
                .retrieve()
                .bodyToMono(PricingResponse.class)
                .timeout(Duration.ofSeconds(5))  // Timeout de 5 segundos
                .retryWhen(
                    Retry.backoff(3, Duration.ofSeconds(1))  // 3 intentos, delay inicial 1s
                         .maxBackoff(Duration.ofSeconds(8))  // Delay máximo 8s
                         .filter(ex -> !(ex instanceof WebClientResponseException.NotFound))
                         .doBeforeRetry(signal -> log.warn(
                             "[PricingClient] Reintento #{} para producto {} — causa: {}",
                             signal.totalRetries() + 1, productId,
                             signal.failure().getMessage()))
                )
                .map(PricingResponse::price)
                .doOnNext(price -> log.info(
                    "[PricingClient] Precio externo para {}: {}", productId, price))
                .onErrorResume(ex -> {
                    // Fallback: usar el precio almacenado localmente
                    log.warn("[PricingClient] Servicio externo no disponible para {}. " +
                             "Usando precio local: {} — Error: {}",
                             productId, localPrice, ex.getMessage());
                    return Mono.just(localPrice);
                });
    }

    // DTO de respuesta del servicio de pricing
    public record PricingResponse(String productId, BigDecimal price, String currency) {}
}
```

**5.2 Integrar el PricingClient en el servicio R2DBC**

Agrega el siguiente método en `R2dbcProductService.java` (añadir `PricingClient` como dependencia inyectada):

```java
// Agregar campo en R2dbcProductService:
private final PricingClient pricingClient;

// Agregar método:
/**
 * Obtiene un producto con el precio actualizado desde el servicio externo.
 * Si el servicio externo falla después de los reintentos, se usa el precio local.
 */
public Mono<Product> findByIdWithUpdatedPrice(String id) {
    return findById(id)
            .flatMap(product ->
                pricingClient.getUpdatedPrice(id, product.getPrice())
                    .map(updatedPrice -> {
                        product.setPrice(updatedPrice);
                        return product;
                    })
            );
}
```

**5.3 Agregar endpoint en el controller**

```java
// Agregar en ProductController.java:
/**
 * GET /products/{id}/price
 * Obtiene el producto con precio actualizado desde el servicio externo.
 * Incluye fallback al precio local si el servicio externo no responde.
 */
@GetMapping("/{id}/price")
public Mono<ResponseEntity<Product>> getProductWithPrice(@PathVariable String id) {
    // Nota: este cast solo funciona si el servicio es R2dbcProductService.
    // En un diseño más limpio, el método se añadiría a la interfaz ProductService.
    return productService.findById(id)
            .flatMap(product ->
                pricingClient.getUpdatedPrice(id, product.getPrice())
                    .map(updatedPrice -> {
                        product.setPrice(updatedPrice);
                        return product;
                    })
            )
            .map(ResponseEntity::ok)
            .defaultIfEmpty(ResponseEntity.notFound().build());
}
```

Agrega `PricingClient pricingClient` como campo inyectado en `ProductController`.

**5.4 Crear el test con MockWebServer**

Archivo `src/test/java/com/curso/catalog/client/PricingClientTest.java`:

```java
package com.curso.catalog.client;

import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import org.junit.jupiter.api.*;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.io.IOException;
import java.math.BigDecimal;

class PricingClientTest {

    private MockWebServer mockWebServer;
    private PricingClient pricingClient;

    @BeforeEach
    void setUp() throws IOException {
        mockWebServer = new MockWebServer();
        mockWebServer.start();

        String baseUrl = mockWebServer.url("/").toString();
        pricingClient = new PricingClient(WebClient.builder(), baseUrl);
    }

    @AfterEach
    void tearDown() throws IOException {
        mockWebServer.shutdown();
    }

    @Test
    @DisplayName("Debe retornar precio externo cuando el servicio responde correctamente")
    void shouldReturnExternalPriceOnSuccess() {
        // Preparar respuesta del mock
        mockWebServer.enqueue(new MockResponse()
                .setResponseCode(200)
                .setHeader("Content-Type", "application/json")
                .setBody("""
                    {"productId":"1","price":999.99,"currency":"USD"}
                """));

        BigDecimal localPrice = new BigDecimal("1299.99");

        StepVerifier.create(pricingClient.getUpdatedPrice("1", localPrice))
                .expectNext(new BigDecimal("999.99"))
                .verifyComplete();
    }

    @Test
    @DisplayName("Debe usar precio local como fallback cuando el servicio falla")
    void shouldUseFallbackPriceOnServiceFailure() {
        // Simular 3 fallos consecutivos (agota los reintentos)
        for (int i = 0; i < 4; i++) {
            mockWebServer.enqueue(new MockResponse()
                    .setResponseCode(500)
                    .setBody("Internal Server Error"));
        }

        BigDecimal localPrice = new BigDecimal("1299.99");

        StepVerifier.create(pricingClient.getUpdatedPrice("1", localPrice))
                .expectNext(localPrice)  // Fallback al precio local
                .verifyComplete();
    }

    @Test
    @DisplayName("Debe aplicar fallback inmediato en timeout sin reintentar indefinidamente")
    void shouldApplyFallbackOnTimeout() {
        // Simular respuestas lentas que exceden el timeout
        for (int i = 0; i < 4; i++) {
            mockWebServer.enqueue(new MockResponse()
                    .setResponseCode(200)
                    .setBodyDelay(10, java.util.concurrent.TimeUnit.SECONDS)
                    .setBody("""
                        {"productId":"1","price":999.99,"currency":"USD"}
                    """));
        }

        BigDecimal localPrice = new BigDecimal("1299.99");

        StepVerifier.withVirtualTime(() -> pricingClient.getUpdatedPrice("1", localPrice))
                .thenAwait(java.time.Duration.ofSeconds(60))
                .expectNext(localPrice)
                .verifyComplete();
    }
}
```

**5.5 Ejecutar los tests**

```bash
# Ejecutar solo los tests del PricingClient
mvn test -Dtest=PricingClientTest -pl . -q

# Salida esperada:
# [INFO] Tests run: 3, Failures: 0, Errors: 0, Skipped: 0
```

---

## Validación y Pruebas

### Suite de validación completa

Ejecuta la siguiente secuencia para validar todos los bloques:

```bash
# 1. Iniciar la aplicación con perfil relacional
mvn spring-boot:run -Dspring-boot.run.profiles=relational &
APP_PID=$!
sleep 10  # Esperar a que la aplicación inicie

# 2. Validar endpoint de listado con paginación
echo "=== Paginación ==="
curl -s "http://localhost:8080/products?page=0&size=3" | python3 -m json.tool

# 3. Validar filtrado por categoría
echo "=== Filtrado por categoría ==="
curl -s "http://localhost:8080/products/category/electronics" | python3 -m json.tool

# 4. Validar rango de precios
echo "=== Rango de precios ==="
curl -s "http://localhost:8080/products/price-range?min=50&max=500" | python3 -m json.tool

# 5. Validar creación de producto
echo "=== Crear producto ==="
curl -s -X POST http://localhost:8080/products \
  -H "Content-Type: application/json" \
  -d '{"name":"Webcam HD","description":"Webcam 1080p","category":"peripherals","price":79.99,"stock":100,"active":true}' \
  | python3 -m json.tool

# 6. Validar streaming (recibir 3 eventos y cancelar)
echo "=== Streaming SSE (3 eventos) ==="
curl -N -H "Accept: text/event-stream" http://localhost:8080/products/stream &
STREAM_PID=$!
sleep 3
kill $STREAM_PID 2>/dev/null

# 7. Ejecutar todos los tests
echo "=== Tests ==="
mvn test -q

# Limpiar
kill $APP_PID 2>/dev/null
```

### Criterios de aceptación

| Criterio                                               | Resultado esperado                          |
|--------------------------------------------------------|---------------------------------------------|
| `GET /products?page=0&size=3` devuelve exactamente 3 productos | Array JSON con 3 elementos            |
| `GET /products/stream` emite eventos SSE separados     | `data:{...}` cada ~500 ms                   |
| `POST /products/transfer-inventory` con stock suficiente | HTTP 200 con producto actualizado         |
| `POST /products/transfer-inventory` con stock insuficiente | HTTP 400 y stocks sin cambios          |
| Tests de `PricingClientTest` pasan todos               | 3/3 tests en verde                          |
| Aplicación inicia con perfil `document` sin errores    | Spring context carga correctamente          |

---

## Resolución de Problemas

### Problema 1: La aplicación falla al iniciar con "Cannot load auto-configuration class"

**Síntomas**: Al iniciar con un perfil específico, Spring Boot lanza `IllegalStateException` o `BeanCreationException` relacionado con autoconfiguración de R2DBC o MongoDB.

**Causa**: Las autoconfiguraciones de R2DBC y MongoDB Reactive se activan simultáneamente aunque solo un perfil esté activo. Spring Boot intenta crear beans para ambas tecnologías, pero solo una tiene URL de conexión configurada.

**Solución**:
1. Verifica que el bloque `autoconfigure.exclude` esté correctamente indentado en `application.yml` bajo el perfil correspondiente.
2. Asegúrate de que las anotaciones `@Profile("relational")` y `@Profile("document")` estén presentes en los repositorios y servicios.
3. Si el problema persiste, añade la exclusión directamente en la anotación de la clase principal:

```java
// Alternativa: exclusión explícita en la clase principal
@SpringBootApplication(exclude = {
    // Se activan dinámicamente según el perfil en application.yml
})
```

4. Verifica el perfil activo con:
```bash
curl -s http://localhost:8080/actuator/env | python3 -m json.tool | grep "active"
```

---

### Problema 2: El endpoint `/products/stream` devuelve todos los productos de golpe en lugar de hacer streaming

**Síntomas**: Al llamar a `GET /products/stream`, el cliente recibe todos los datos en una sola respuesta en lugar de recibirlos progresivamente. No se observa el delay de 500 ms entre elementos.

**Causa**: El cliente HTTP (navegador, Postman, curl sin `-N`) está acumulando el buffer antes de mostrar la respuesta. Alternativamente, falta el header `Accept: text/event-stream` en la petición, lo que hace que Spring WebFlux serialice el `Flux` completo como un array JSON.

**Solución**:
1. Usa `curl` con la opción `-N` (no-buffer) y el header correcto:
```bash
curl -N -H "Accept: text/event-stream" http://localhost:8080/products/stream
```
2. Verifica que el endpoint en el controller declare `produces = MediaType.TEXT_EVENT_STREAM_VALUE`.
3. En Postman, configura el header `Accept: text/event-stream` y activa la opción "Send and Download" para ver la respuesta en tiempo real.
4. Para confirmar que el `delayElements` está activo, revisa los logs: deberías ver mensajes de debug cada ~500 ms.

---

## Limpieza del Entorno

```bash
# 1. Detener la aplicación Spring Boot (si está en ejecución)
# Presionar Ctrl+C en la terminal donde corre, o:
pkill -f "spring-boot:run" 2>/dev/null

# 2. Detener y eliminar los contenedores Docker
docker compose down

# 3. Eliminar volúmenes de datos (opcional — borra todos los datos)
docker compose down -v

# 4. Verificar que los contenedores se eliminaron
docker ps -a | grep lab03

# 5. Limpiar artefactos de Maven (opcional)
mvn clean

# 6. Liberar puertos (verificar que 5432, 27017 y 8080 están libres)
lsof -i :5432 -i :27017 -i :8080 2>/dev/null || echo "Puertos libres"
```

> **Nota**: Si planeas continuar con el Lab 04, puedes conservar los contenedores Docker ejecutando solo `docker compose stop` en lugar de `docker compose down`.

---

## Resumen

En este laboratorio construiste una API de catálogo de productos que demuestra los conceptos fundamentales de la persistencia reactiva en Spring Boot:

| Bloque | Concepto clave | Tecnología utilizada |
|--------|---------------|---------------------|
| 1 | Infraestructura Docker con dos motores de BD | Docker Compose, PostgreSQL 16, MongoDB 7.0 |
| 2 | Repositorios reactivos y consultas derivadas | `ReactiveCrudRepository`, `ReactiveMongoRepository`, `@Query` |
| 3 | Paginación reactiva y streaming SSE | `Pageable`, `Flux.delayElements`, `text/event-stream` |
| 4 | Transacciones reactivas y rollback | `@Transactional` R2DBC, operaciones atómicas MongoDB |
| 5 | WebClient resiliente con retry y fallback | `Retry.backoff`, `.timeout()`, `.onErrorResume()`, `MockWebServer` |

### Puntos clave para recordar

- **Spring Data R2DBC no es JPA**: no existe lazy loading ni caché de primer nivel. Las relaciones entre tablas deben manejarse con joins manuales usando `DatabaseClient` o desnormalizando el modelo.
- **La uniformidad de la API reactiva** (`Mono`/`Flux`) permite intercambiar el motor de persistencia sin modificar el controller ni la lógica de negocio.
- **`@Transactional` en R2DBC** propaga el contexto transaccional a través del pipeline reactivo; nunca uses `block()` dentro de un método transaccional reactivo.
- **El streaming SSE** requiere el header `Accept: text/event-stream` en el cliente y `produces = MediaType.TEXT_EVENT_STREAM_VALUE` en el servidor.
- **El retry exponencial** protege al sistema de fallos transitorios en servicios externos; el fallback garantiza degradación elegante.

### Recursos adicionales

- [Spring Data R2DBC — Referencia oficial](https://docs.spring.io/spring-data/relational/reference/r2dbc.html)
- [Spring Data MongoDB Reactive — Referencia oficial](https://docs.spring.io/spring-data/mongodb/reference/mongodb/reactive-mongo-repositories.html)
- [Project Reactor — Referencia de operadores](https://projectreactor.io/docs/core/release/reference/)
- [MockWebServer — OkHttp](https://github.com/square/okhttp/tree/master/mockwebserver)
- [R2DBC — Especificación oficial](https://r2dbc.io/)

---
LAB_END---
