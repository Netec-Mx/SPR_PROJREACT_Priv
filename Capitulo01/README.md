# Primeros pipelines reactivos con operadores

## Metadatos

| Campo            | Valor                                      |
|------------------|--------------------------------------------|
| **Duración**     | 90 minutos                                 |
| **Complejidad**  | Media                                      |
| **Nivel Bloom**  | Aplicar (*Apply*)                          |
| **Tecnologías**  | Project Reactor 3.6, Spring Boot 3.3, Java 21, Maven 3.9 |

---

## Descripción General

En esta práctica construirás una serie de pipelines reactivos progresivamente más complejos usando Project Reactor 3.6 dentro de un proyecto Spring Boot standalone (sin capa web). Partirás de la motivación conceptual vista en la lección 1.1 —el contraste entre el modelo bloqueante y el no bloqueante— y la llevarás al código: crearás `Mono` y `Flux`, aplicarás operadores de transformación y combinación, introducirás temporización asíncrona y configurarás backpressure explícito. Cada ejercicio refuerza por qué `block()` es un antipatrón en producción y cuándo está justificado en contexto de pruebas de consola.

---

## Objetivos de Aprendizaje

Al finalizar esta práctica serás capaz de:

- [ ] Distinguir el modelo bloqueante del no bloqueante y argumentar cuándo aplicar programación reactiva en un escenario real.
- [ ] Crear instancias de `Mono` y `Flux` con diferentes factories (`just`, `fromIterable`, `generate`, `create`) y suscribirte a ellas de forma explícita.
- [ ] Aplicar operadores de transformación (`map`, `flatMap`, `filter`), combinación (`merge`, `zip`, `concat`) y temporización (`delayElements`, `interval`) en pipelines reactivos.
- [ ] Implementar estrategias de backpressure con `limitRate`, `onBackpressureBuffer` y `onBackpressureDrop`.
- [ ] Manejar errores dentro del pipeline usando `onErrorReturn`, `onErrorResume` y `retry`.

---

## Prerrequisitos

### Conocimiento previo
- Java 8+ Streams y expresiones lambda (uso básico de `map`, `filter`, `collect`).
- Patrón Observer y programación orientada a objetos.
- Creación y ejecución de un proyecto Spring Boot desde Spring Initializr.
- Conceptos básicos de concurrencia: hilos, callbacks y el problema del *callback hell*.

### Acceso y herramientas requeridas
- JDK 21 LTS instalado y configurado (`JAVA_HOME` apuntando a JDK 21).
- Apache Maven 3.9.x disponible en el `PATH`.
- IntelliJ IDEA 2024.1+ (Community o Ultimate) instalado.
- Conexión a Internet para descargar dependencias de Maven Central.
- Git 2.40+ (opcional para versionar el proyecto).

---

## Entorno del Laboratorio

### Verificación del entorno antes de comenzar

Ejecuta los siguientes comandos en una terminal para confirmar que el entorno está listo:

```bash
# Verificar versión de Java (debe mostrar 21.x)
java -version

# Verificar versión de Maven (debe mostrar 3.9.x)
mvn -version

# Verificar JAVA_HOME
echo $JAVA_HOME        # Linux/macOS
echo %JAVA_HOME%       # Windows CMD
```

**Salida esperada (ejemplo):**
```
openjdk version "21.0.3" 2024-04-16 LTS
Apache Maven 3.9.6
```

> ⚠️ **ANTIPATRÓN CRÍTICO — BLOQUEO EN EL EVENT LOOP**: A lo largo de esta práctica usarás `block()` y `blockFirst()` **exclusivamente en el método `main()` o en tests de consola** para observar resultados. Nunca uses `block()` dentro de un controller WebFlux ni dentro de un pipeline reactivo en producción. Este punto se reforzará en cada lab del curso.

---

## Pasos del Laboratorio

---

### Paso 1 — Crear el proyecto Spring Boot standalone

**Objetivo:** Generar la estructura base del proyecto con las dependencias necesarias de Project Reactor.

#### Instrucciones

1. Abre tu navegador y dirígete a [https://start.spring.io](https://start.spring.io).

2. Configura el proyecto con los siguientes valores:

   | Campo            | Valor                          |
   |------------------|--------------------------------|
   | Project          | Maven                          |
   | Language         | Java                           |
   | Spring Boot      | 3.3.x (la más reciente estable)|
   | Group            | com.curso.reactivo              |
   | Artifact         | reactor-lab01                  |
   | Name             | reactor-lab01                  |
   | Package name     | com.curso.reactivo.lab01       |
   | Packaging        | Jar                            |
   | Java             | 21                             |

3. En la sección **Dependencies**, agrega únicamente:
   - `Spring Reactive Web` — esto incluye Project Reactor y Netty (aunque no usaremos la capa web, trae Reactor como dependencia transitiva).

   > **Nota:** Alternativamente puedes agregar solo `Reactor Core` si no deseas incluir WebFlux. Para este lab, `Spring Reactive Web` es suficiente y facilita los labs posteriores.

4. Haz clic en **Generate** y descarga el archivo `.zip`.

5. Descomprime el archivo en tu directorio de trabajo (por ejemplo `~/proyectos/reactor-lab01`) y ábrelo en IntelliJ IDEA.

6. Verifica que el `pom.xml` contiene la dependencia de Reactor:

```xml
<!-- Fragmento relevante del pom.xml generado -->
<parent>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-starter-parent</artifactId>
    <version>3.3.x</version>
</parent>

<dependencies>
    <dependency>
        <groupId>org.springframework.boot</groupId>
        <artifactId>spring-boot-starter-webflux</artifactId>
    </dependency>
    <dependency>
        <groupId>org.springframework.boot</groupId>
        <artifactId>spring-boot-starter-test</artifactId>
        <scope>test</scope>
    </dependency>
</dependencies>
```

7. Agrega también la dependencia de **Reactor Test** para los ejercicios de verificación:

```xml
<dependency>
    <groupId>io.projectreactor</groupId>
    <artifactId>reactor-test</artifactId>
    <scope>test</scope>
</dependency>
```

8. Desde la terminal, en la raíz del proyecto, ejecuta:

```bash
mvn dependency:resolve -q
```

#### Salida esperada
```
BUILD SUCCESS
```

#### Verificación
Abre el archivo `pom.xml` en IntelliJ y confirma que las dependencias `spring-boot-starter-webflux` y `reactor-test` aparecen en el árbol de dependencias (pestaña **Maven → Dependencies**).

---

### Paso 2 — Ejercicio 1: Mono y Flux básicos (modelo bloqueante vs. no bloqueante)

**Objetivo:** Crear instancias de `Mono` y `Flux` con factories básicas y observar la diferencia conceptual entre obtener un valor de forma bloqueante y suscribirse de forma reactiva.

#### Instrucciones

1. Crea el paquete `com.curso.reactivo.lab01.ejercicio1` y dentro crea la clase `EjercicioMonoFlux`:

```java
package com.curso.reactivo.lab01.ejercicio1;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;

public class EjercicioMonoFlux {

    public static void main(String[] args) {

        // ─── 1. Mono simple ───────────────────────────────────────────────
        System.out.println("=== MONO SIMPLE ===");

        // Mono: representa 0 o 1 elemento (promesa de un valor futuro)
        Mono<String> monoProducto = Mono.just("Laptop Pro X1");

        // Suscripción explícita: el pipeline NO se ejecuta hasta que hay un suscriptor
        monoProducto.subscribe(
            valor  -> System.out.println("Producto recibido: " + valor),
            error  -> System.err.println("Error: " + error.getMessage()),
            ()     -> System.out.println("Mono completado.")
        );

        // ─── 2. Mono vacío ────────────────────────────────────────────────
        System.out.println("\n=== MONO VACÍO ===");

        Mono<String> monoVacio = Mono.empty();
        monoVacio.subscribe(
            valor -> System.out.println("Valor: " + valor),        // nunca se llama
            error -> System.err.println("Error: " + error),
            ()    -> System.out.println("Mono vacío completado.")   // sí se llama
        );

        // ─── 3. Flux desde lista ──────────────────────────────────────────
        System.out.println("\n=== FLUX DESDE LISTA ===");

        List<String> catalogo = List.of("Laptop", "Mouse", "Teclado", "Monitor", "Auriculares");
        Flux<String> fluxProductos = Flux.fromIterable(catalogo);

        fluxProductos.subscribe(
            item      -> System.out.println("Ítem: " + item),
            error     -> System.err.println("Error: " + error),
            ()        -> System.out.println("Flux completado. Total ítems procesados: " + catalogo.size())
        );

        // ─── 4. Comparación: bloqueo vs. suscripción ─────────────────────
        System.out.println("\n=== BLOQUEANTE (solo para demostración/tests) ===");

        // block() es ACEPTABLE aquí porque estamos en main() para observar resultados.
        // NUNCA uses block() en un controller WebFlux o en un pipeline de producción.
        String productoObtenido = Mono.just("Teclado Mecánico").block();
        System.out.println("Valor obtenido con block(): " + productoObtenido);

        System.out.println("\n=== NO BLOQUEANTE (forma correcta en producción) ===");
        // En producción, retornarías el Mono y el framework se suscribe:
        Mono<String> monoParaProduccion = Mono.just("Teclado Mecánico");
        // El framework (WebFlux) se suscribiría aquí automáticamente
        monoParaProduccion.subscribe(v -> System.out.println("Procesado reactivamente: " + v));
    }
}
```

2. Ejecuta la clase haciendo clic derecho → **Run 'EjercicioMonoFlux.main()'** en IntelliJ.

#### Salida esperada
```
=== MONO SIMPLE ===
Producto recibido: Laptop Pro X1
Mono completado.

=== MONO VACÍO ===
Mono vacío completado.

=== FLUX DESDE LISTA ===
Ítem: Laptop
Ítem: Mouse
Ítem: Teclado
Ítem: Monitor
Ítem: Auriculares
Flux completado. Total ítems procesados: 5

=== BLOQUEANTE (solo para demostración/tests) ===
Valor obtenido con block(): Teclado Mecánico

=== NO BLOQUEANTE (forma correcta en producción) ===
Procesado reactivamente: Teclado Mecánico
```

#### Verificación
- Confirma que el `Mono.empty()` **no imprime ningún valor** pero sí dispara el callback de completación.
- Confirma que el `Flux.fromIterable()` emite los 5 ítems en orden y luego dispara el callback de completación.
- Observa que `block()` devuelve el valor directamente (comportamiento sincrónico), mientras que `subscribe()` registra un callback (comportamiento asíncrono).

---

### Paso 3 — Ejercicio 2: Factories avanzadas (`generate` y `create`)

**Objetivo:** Comprender cómo crear fuentes de datos programáticas con `Flux.generate()` y `Flux.create()` para modelar escenarios donde los datos no están disponibles de antemano.

#### Instrucciones

1. Crea la clase `EjercicioFactories` en el paquete `ejercicio2`:

```java
package com.curso.reactivo.lab01.ejercicio2;

import reactor.core.publisher.Flux;
import reactor.core.publisher.SynchronousSink;

import java.util.concurrent.atomic.AtomicInteger;

public class EjercicioFactories {

    public static void main(String[] args) {

        // ─── 1. Flux.generate: fuente síncrona con estado ─────────────────
        // Útil para generar secuencias infinitas o calculadas
        System.out.println("=== FLUX.GENERATE (IDs de pedido secuenciales) ===");

        Flux<String> generadorPedidos = Flux.generate(
            () -> 1000,                          // estado inicial: primer ID de pedido
            (estado, sink) -> {
                sink.next("PEDIDO-" + estado);   // emite el siguiente elemento
                if (estado >= 1004) {
                    sink.complete();             // termina después de 5 pedidos
                }
                return estado + 1;               // nuevo estado para la próxima llamada
            }
        );

        generadorPedidos.subscribe(
            pedido -> System.out.println("Generado: " + pedido),
            err    -> System.err.println("Error: " + err),
            ()     -> System.out.println("Generación completada.")
        );

        // ─── 2. Flux.create: fuente asíncrona con FluxSink ────────────────
        // Útil para adaptar APIs basadas en callbacks (listeners, eventos externos)
        System.out.println("\n=== FLUX.CREATE (simulando eventos de inventario) ===");

        Flux<String> eventosInventario = Flux.create(sink -> {
            // Simulamos una fuente de eventos externa (ej: listener de mensajería)
            String[] eventos = {
                "STOCK_BAJO:Laptop",
                "REABASTECIMIENTO:Mouse",
                "AGOTADO:Teclado",
                "STOCK_BAJO:Monitor"
            };

            for (String evento : eventos) {
                if (sink.isCancelled()) break;   // respetar cancelación
                sink.next(evento);
            }
            sink.complete();
        });

        eventosInventario.subscribe(
            evento -> System.out.println("Evento recibido: " + evento),
            err    -> System.err.println("Error: " + err),
            ()     -> System.out.println("Stream de eventos completado.")
        );

        // ─── 3. Flux.range y Flux.just para datos conocidos ───────────────
        System.out.println("\n=== FLUX.RANGE (IDs del 1 al 5) ===");

        Flux.range(1, 5)
            .subscribe(id -> System.out.println("ID: " + id));
    }
}
```

2. Ejecuta la clase y observa la salida.

#### Salida esperada
```
=== FLUX.GENERATE (IDs de pedido secuenciales) ===
Generado: PEDIDO-1000
Generado: PEDIDO-1001
Generado: PEDIDO-1002
Generado: PEDIDO-1003
Generado: PEDIDO-1004
Generación completada.

=== FLUX.CREATE (simulando eventos de inventario) ===
Evento recibido: STOCK_BAJO:Laptop
Evento recibido: REABASTECIMIENTO:Mouse
Evento recibido: AGOTADO:Teclado
Evento recibido: STOCK_BAJO:Monitor
Stream de eventos completado.

=== FLUX.RANGE (IDs del 1 al 5) ===
ID: 1
ID: 2
ID: 3
ID: 4
ID: 5
```

#### Verificación
- `Flux.generate()` emite exactamente 5 pedidos (PEDIDO-1000 a PEDIDO-1004) y se completa.
- `Flux.create()` adapta el array de strings como si fuera una fuente de eventos externa.
- Nota la diferencia conceptual: `generate` es **síncrono** (un elemento a la vez, controlado por el sink), mientras que `create` es **asíncrono** (puede emitir múltiples elementos desde distintos hilos).

---

### Paso 4 — Ejercicio 3: Operadores de transformación (`map`, `flatMap`, `filter`)

**Objetivo:** Aplicar los operadores de transformación más comunes para enriquecer y filtrar datos en un pipeline reactivo, modelando una capa de servicio de productos.

#### Instrucciones

1. Primero, crea los records de dominio en el paquete `com.curso.reactivo.lab01.modelo`:

```java
package com.curso.reactivo.lab01.modelo;

public record Producto(String id, String nombre, double precio, String categoria) {}
public record ProductoEnriquecido(Producto producto, double precioConIva, String etiqueta) {}
```

2. Crea la clase `EjercicioTransformacion` en el paquete `ejercicio3`:

```java
package com.curso.reactivo.lab01.ejercicio3;

import com.curso.reactivo.lab01.modelo.Producto;
import com.curso.reactivo.lab01.modelo.ProductoEnriquecido;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;

public class EjercicioTransformacion {

    // Simula una llamada reactiva a un servicio externo de enriquecimiento
    private static Mono<ProductoEnriquecido> enriquecerProducto(Producto p) {
        double iva = p.precio() * 1.19;
        String etiqueta = p.precio() > 500 ? "PREMIUM" : "ESTÁNDAR";
        return Mono.just(new ProductoEnriquecido(p, iva, etiqueta));
    }

    public static void main(String[] args) {

        List<Producto> productos = List.of(
            new Producto("P001", "Laptop Pro",    1200.0, "ELECTRÓNICA"),
            new Producto("P002", "Mouse Inalámbrico", 35.0, "PERIFÉRICO"),
            new Producto("P003", "Monitor 4K",    850.0, "ELECTRÓNICA"),
            new Producto("P004", "Teclado Mecánico", 120.0, "PERIFÉRICO"),
            new Producto("P005", "Auriculares BT", 200.0, "AUDIO")
        );

        // ─── 1. map: transformación sincrónica 1-a-1 ──────────────────────
        System.out.println("=== MAP: nombres en mayúsculas ===");

        Flux.fromIterable(productos)
            .map(p -> p.nombre().toUpperCase())
            .subscribe(nombre -> System.out.println("  " + nombre));

        // ─── 2. filter: filtrado por condición ────────────────────────────
        System.out.println("\n=== FILTER: solo productos > $100 ===");

        Flux.fromIterable(productos)
            .filter(p -> p.precio() > 100)
            .subscribe(p -> System.out.printf("  %s - $%.2f%n", p.nombre(), p.precio()));

        // ─── 3. flatMap: transformación asíncrona 1-a-N ───────────────────
        // flatMap suscribe a cada Mono/Flux interno y aplana los resultados
        // IMPORTANTE: flatMap puede reordenar elementos (no garantiza orden)
        System.out.println("\n=== FLAT_MAP: enriquecimiento reactivo (puede reordenar) ===");

        Flux.fromIterable(productos)
            .filter(p -> p.categoria().equals("ELECTRÓNICA"))
            .flatMap(EjercicioTransformacion::enriquecerProducto)
            .subscribe(pe -> System.out.printf(
                "  %s | Precio+IVA: $%.2f | Etiqueta: %s%n",
                pe.producto().nombre(), pe.precioConIva(), pe.etiqueta()
            ));

        // ─── 4. concatMap: transformación asíncrona que PRESERVA el orden ─
        System.out.println("\n=== CONCAT_MAP: enriquecimiento reactivo (orden garantizado) ===");

        Flux.fromIterable(productos)
            .filter(p -> p.categoria().equals("ELECTRÓNICA"))
            .concatMap(EjercicioTransformacion::enriquecerProducto)
            .subscribe(pe -> System.out.printf(
                "  %s | Precio+IVA: $%.2f | Etiqueta: %s%n",
                pe.producto().nombre(), pe.precioConIva(), pe.etiqueta()
            ));

        // ─── 5. Cadena completa: filter → map → flatMap ───────────────────
        System.out.println("\n=== PIPELINE COMPLETO: ELECTRÓNICA → enriquecer → formatear ===");

        Flux.fromIterable(productos)
            .filter(p -> p.categoria().equals("ELECTRÓNICA"))
            .map(p -> new Producto(p.id(), p.nombre().toUpperCase(), p.precio(), p.categoria()))
            .flatMap(EjercicioTransformacion::enriquecerProducto)
            .subscribe(pe -> System.out.printf(
                "  [%s] %s → $%.2f (con IVA)%n",
                pe.etiqueta(), pe.producto().nombre(), pe.precioConIva()
            ));
    }
}
```

#### Salida esperada
```
=== MAP: nombres en mayúsculas ===
  LAPTOP PRO
  MOUSE INALÁMBRICO
  MONITOR 4K
  TECLADO MECÁNICO
  AURICULARES BT

=== FILTER: solo productos > $100 ===
  Laptop Pro - $1200.00
  Monitor 4K - $850.00
  Teclado Mecánico - $120.00
  Auriculares BT - $200.00

=== FLAT_MAP: enriquecimiento reactivo (puede reordenar) ===
  Laptop Pro | Precio+IVA: $1428.00 | Etiqueta: PREMIUM
  Monitor 4K | Precio+IVA: $1011.50 | Etiqueta: PREMIUM

=== CONCAT_MAP: enriquecimiento reactivo (orden garantizado) ===
  Laptop Pro | Precio+IVA: $1428.00 | Etiqueta: PREMIUM
  Monitor 4K | Precio+IVA: $1011.50 | Etiqueta: PREMIUM

=== PIPELINE COMPLETO: ELECTRÓNICA → enriquecer → formatear ===
  [PREMIUM] LAPTOP PRO → $1428.00 (con IVA)
  [PREMIUM] MONITOR 4K → $1011.50 (con IVA)
```

#### Verificación
- `map` transforma de forma sincrónica (1-a-1): cada elemento entra y sale como un elemento distinto.
- `filter` elimina los productos con precio ≤ $100 (Mouse e implícitamente Teclado y Auriculares en la sección de ELECTRÓNICA).
- `flatMap` y `concatMap` producen el mismo resultado en este caso síncrono; la diferencia se manifiesta con operaciones verdaderamente asíncronas (con delays).

---

### Paso 5 — Ejercicio 4: Operadores de combinación (`merge`, `zip`, `concat`)

**Objetivo:** Combinar múltiples fuentes reactivas para simular el patrón de composición paralela descrito en la lección 1.1 (obtener producto + precio + inventario en paralelo).

#### Instrucciones

1. Crea la clase `EjercicioCombinacion` en el paquete `ejercicio4`:

```java
package com.curso.reactivo.lab01.ejercicio4;

import com.curso.reactivo.lab01.modelo.Producto;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;

public class EjercicioCombinacion {

    // Servicios simulados con latencia artificial
    private static Mono<String> obtenerPrecio(String productoId) {
        return Mono.just("$1200.00")
                   .delayElement(Duration.ofMillis(100)); // simula latencia de red
    }

    private static Mono<Integer> verificarInventario(String productoId) {
        return Mono.just(42)
                   .delayElement(Duration.ofMillis(150)); // simula latencia de BD
    }

    private static Mono<String> obtenerCategoria(String productoId) {
        return Mono.just("ELECTRÓNICA")
                   .delayElement(Duration.ofMillis(80));  // simula latencia de caché
    }

    public static void main(String[] args) throws InterruptedException {

        // ─── 1. Mono.zip: composición paralela (el patrón de la lección 1.1) ──
        System.out.println("=== MONO.ZIP: composición paralela de 3 servicios ===");

        long inicio = System.currentTimeMillis();

        Mono<String> resultado = Mono.zip(
            obtenerPrecio("P001"),
            verificarInventario("P001"),
            obtenerCategoria("P001")
        ).map(tuple -> String.format(
            "Producto P001 | Precio: %s | Stock: %d | Categoría: %s",
            tuple.getT1(), tuple.getT2(), tuple.getT3()
        ));

        // block() justificado: estamos en main() para observar el resultado
        String detalle = resultado.block();
        long duracion = System.currentTimeMillis() - inicio;

        System.out.println("  " + detalle);
        System.out.printf("  Tiempo total: %d ms (esperado ~150 ms, no ~330 ms)%n%n", duracion);

        // ─── 2. Flux.merge: combina múltiples Flux sin garantía de orden ──
        System.out.println("=== FLUX.MERGE: fuentes intercaladas (sin orden garantizado) ===");

        Flux<String> electrónica = Flux.just("Laptop", "Monitor")
                                       .delayElements(Duration.ofMillis(100));
        Flux<String> periféricos = Flux.just("Mouse", "Teclado")
                                       .delayElements(Duration.ofMillis(75));

        Flux.merge(electrónica, periféricos)
            .doOnNext(item -> System.out.println("  Recibido: " + item))
            .blockLast(); // block() justificado: observar en main()

        // ─── 3. Flux.concat: combina en secuencia (orden garantizado) ─────
        System.out.println("\n=== FLUX.CONCAT: fuentes en secuencia (orden garantizado) ===");

        Flux<String> primera  = Flux.just("A", "B", "C");
        Flux<String> segunda  = Flux.just("D", "E", "F");

        Flux.concat(primera, segunda)
            .subscribe(item -> System.out.print(item + " "));
        System.out.println("\n  (concat preserva el orden: primero toda la primera fuente)");

        // ─── 4. Flux.zip: combina elemento a elemento ─────────────────────
        System.out.println("\n=== FLUX.ZIP: emparejamiento elemento a elemento ===");

        Flux<String> nombres  = Flux.just("Laptop", "Mouse", "Monitor");
        Flux<Double> precios  = Flux.just(1200.0, 35.0, 850.0);

        Flux.zip(nombres, precios)
            .map(tuple -> tuple.getT1() + " → $" + tuple.getT2())
            .subscribe(par -> System.out.println("  " + par));
    }
}
```

2. Ejecuta la clase.

#### Salida esperada
```
=== MONO.ZIP: composición paralela de 3 servicios ===
  Producto P001 | Precio: $1200.00 | Stock: 42 | Categoría: ELECTRÓNICA
  Tiempo total: ~155 ms (esperado ~150 ms, no ~330 ms)

=== FLUX.MERGE: fuentes intercaladas (sin orden garantizado) ===
  Recibido: Mouse
  Recibido: Teclado
  Recibido: Laptop
  Recibido: Monitor

=== FLUX.CONCAT: fuentes en secuencia (orden garantizado) ===
A B C D E F
  (concat preserva el orden: primero toda la primera fuente)

=== FLUX.ZIP: emparejamiento elemento a elemento ===
  Laptop → $1200.0
  Mouse → $35.0
  Monitor → $850.0
```

> **Observación clave:** El tiempo de `Mono.zip` es ~150 ms (la latencia de la operación más lenta), **no** ~330 ms (la suma de las tres latencias). Esto demuestra la ventaja de composición paralela descrita en la lección 1.1.

#### Verificación
- El tiempo total de `Mono.zip` debe ser cercano a 150 ms (no 330 ms), confirmando la ejecución paralela.
- `Flux.merge` puede mostrar los ítems de `periféricos` antes que los de `electrónica` (los periféricos tienen menor delay: 75 ms vs 100 ms).
- `Flux.concat` **siempre** muestra A, B, C antes que D, E, F independientemente de delays.

---

### Paso 6 — Ejercicio 5: Temporización con `delayElements` e `interval`

**Objetivo:** Usar operadores de temporización para observar la naturaleza asíncrona de los pipelines reactivos y simular fuentes de eventos en tiempo real.

#### Instrucciones

1. Crea la clase `EjercicioTemporalizacion` en el paquete `ejercicio5`:

```java
package com.curso.reactivo.lab01.ejercicio5;

import reactor.core.publisher.Flux;

import java.time.Duration;

public class EjercicioTemporalizacion {

    public static void main(String[] args) throws InterruptedException {

        // ─── 1. delayElements: introduce latencia entre elementos ──────────
        System.out.println("=== DELAY_ELEMENTS: simulando stream de sensores IoT ===");

        Flux<String> sensorTemperatura = Flux.just(
            "22.5°C", "23.1°C", "22.8°C", "24.0°C", "23.5°C"
        ).delayElements(Duration.ofMillis(300)); // lectura cada 300 ms

        sensorTemperatura
            .doOnNext(lectura -> System.out.println(
                "  [" + System.currentTimeMillis() % 10000 + " ms] Lectura: " + lectura
            ))
            .blockLast(); // block() justificado: esperar en main() para ver resultados

        // ─── 2. Flux.interval: fuente infinita con intervalo fijo ──────────
        System.out.println("\n=== FLUX.INTERVAL: ticks cada 200 ms (tomamos solo 5) ===");

        Flux.interval(Duration.ofMillis(200))
            .take(5)                              // tomamos solo los primeros 5 ticks
            .map(tick -> "Tick #" + tick + " → evento-" + (tick * 100))
            .doOnNext(msg -> System.out.println("  " + msg))
            .blockLast();

        // ─── 3. Combinación: interval + map para simular precios en tiempo real ──
        System.out.println("\n=== PRECIOS EN TIEMPO REAL (simulado con interval) ===");

        double[] precioBase = {100.0};

        Flux.interval(Duration.ofMillis(250))
            .take(6)
            .map(tick -> {
                // Simula fluctuación de precio
                precioBase[0] += (Math.random() * 10 - 5);
                return String.format("BTC/USD: $%.2f", precioBase[0]);
            })
            .subscribe(precio -> System.out.println("  " + precio));

        // Esperamos a que el interval termine (6 ticks × 250 ms = ~1500 ms)
        Thread.sleep(2000);

        System.out.println("\nSimulación completada.");
    }
}
```

2. Ejecuta la clase y observa los timestamps en la salida.

#### Salida esperada
```
=== DELAY_ELEMENTS: simulando stream de sensores IoT ===
  [1234 ms] Lectura: 22.5°C
  [1534 ms] Lectura: 23.1°C
  [1834 ms] Lectura: 22.8°C
  [2134 ms] Lectura: 24.0°C
  [2434 ms] Lectura: 23.5°C

=== FLUX.INTERVAL: ticks cada 200 ms (tomamos solo 5) ===
  Tick #0 → evento-0
  Tick #1 → evento-100
  Tick #2 → evento-200
  Tick #3 → evento-300
  Tick #4 → evento-400

=== PRECIOS EN TIEMPO REAL (simulado con interval) ===
  BTC/USD: $97.43
  BTC/USD: $103.21
  BTC/USD: $98.76
  ...
Simulación completada.
```

#### Verificación
- Los timestamps de `delayElements` deben mostrar diferencias de ~300 ms entre lecturas.
- `Flux.interval` emite exactamente 5 ticks (0 al 4) gracias al operador `take(5)`.
- Sin el `Thread.sleep(2000)` al final, el programa terminaría antes de que el `interval` emita todos los ticks (porque `interval` usa un scheduler de fondo). Esto ilustra la naturaleza asíncrona.

---

### Paso 7 — Ejercicio 6: Backpressure (`limitRate`, `onBackpressureBuffer`, `onBackpressureDrop`)

**Objetivo:** Implementar estrategias de backpressure para controlar una fuente de datos de alta frecuencia y evitar que un suscriptor lento sea desbordado.

#### Instrucciones

1. Crea la clase `EjercicioBackpressure` en el paquete `ejercicio6`:

```java
package com.curso.reactivo.lab01.ejercicio6;

import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

public class EjercicioBackpressure {

    public static void main(String[] args) throws InterruptedException {

        // ─── 1. limitRate: controla la demanda del suscriptor ─────────────
        // El suscriptor solicita elementos en lotes de N para no ser desbordado
        System.out.println("=== LIMIT_RATE: solicitar elementos en lotes de 3 ===");

        Flux.range(1, 12)
            .doOnRequest(n -> System.out.println("  Suscriptor solicitó: " + n + " elementos"))
            .limitRate(3)       // el suscriptor solicita de 3 en 3
            .subscribe(n -> System.out.println("  Procesado: " + n));

        // ─── 2. onBackpressureBuffer: almacena elementos en buffer ─────────
        System.out.println("\n=== ON_BACKPRESSURE_BUFFER: buffer de tamaño 5 ===");

        AtomicInteger descartados = new AtomicInteger(0);

        Flux.range(1, 20)
            .onBackpressureBuffer(
                5,                                          // tamaño máximo del buffer
                descartado -> {
                    descartados.incrementAndGet();
                    System.out.println("  ⚠ Descartado por buffer lleno: " + descartado);
                }
            )
            .publishOn(Schedulers.boundedElastic())         // procesa en hilo diferente (suscriptor lento)
            .doOnNext(n -> {
                try { Thread.sleep(50); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            })
            .subscribe(
                n   -> System.out.println("  Procesado: " + n),
                err -> System.err.println("  Error: " + err.getMessage())
            );

        Thread.sleep(2000); // esperar procesamiento
        System.out.println("  Total descartados por buffer lleno: " + descartados.get());

        // ─── 3. onBackpressureDrop: descarta elementos cuando el suscriptor no puede más ──
        System.out.println("\n=== ON_BACKPRESSURE_DROP: descartar cuando no hay demanda ===");

        CountDownLatch latch = new CountDownLatch(1);
        AtomicInteger procesados = new AtomicInteger(0);
        AtomicInteger omitidos   = new AtomicInteger(0);

        Flux.interval(Duration.ofMillis(10))    // produce 1 elemento cada 10 ms (rápido)
            .take(30)
            .onBackpressureDrop(dropped -> omitidos.incrementAndGet())
            .publishOn(Schedulers.boundedElastic())
            .doOnNext(n -> {
                try { Thread.sleep(100); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
                procesados.incrementAndGet();   // suscriptor procesa 1 cada 100 ms (lento)
            })
            .doOnComplete(() -> latch.countDown())
            .subscribe(
                n   -> {},
                err -> System.err.println("  Error: " + err.getMessage())
            );

        latch.await();
        System.out.printf("  Procesados: %d | Omitidos por drop: %d%n",
            procesados.get(), omitidos.get());
    }
}
```

2. Ejecuta la clase y observa el comportamiento de cada estrategia.

#### Salida esperada
```
=== LIMIT_RATE: solicitar elementos en lotes de 3 ===
  Suscriptor solicitó: 3 elementos
  Procesado: 1
  Procesado: 2
  Procesado: 3
  Suscriptor solicitó: 3 elementos
  Procesado: 4
  ...

=== ON_BACKPRESSURE_BUFFER: buffer de tamaño 5 ===
  Procesado: 1
  Procesado: 2
  ⚠ Descartado por buffer lleno: 8
  ⚠ Descartado por buffer lleno: 9
  ...
  Total descartados por buffer lleno: [número > 0]

=== ON_BACKPRESSURE_DROP: descartar cuando no hay demanda ===
  Procesados: [número pequeño ~3-5] | Omitidos por drop: [número grande ~25-27]
```

#### Verificación
- `limitRate(3)` muestra solicitudes en lotes de 3 en el log de `doOnRequest`.
- `onBackpressureBuffer` procesa algunos elementos y descarta los que exceden el buffer de 5.
- `onBackpressureDrop` muestra que el suscriptor lento (100 ms) procesa muy pocos elementos de la fuente rápida (10 ms), descartando la mayoría.

---

### Paso 8 — Ejercicio 7: Manejo de errores (`onErrorReturn`, `onErrorResume`, `retry`)

**Objetivo:** Implementar estrategias de recuperación de errores dentro del pipeline reactivo sin romper el flujo de datos.

#### Instrucciones

1. Crea la clase `EjercicioErrores` en el paquete `ejercicio7`:

```java
package com.curso.reactivo.lab01.ejercicio7;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

public class EjercicioErrores {

    private static final AtomicInteger intentos = new AtomicInteger(0);

    // Simula un servicio externo que falla intermitentemente
    private static Mono<String> servicioInestable(String productoId) {
        int intento = intentos.incrementAndGet();
        System.out.println("    → Intento #" + intento + " para " + productoId);

        if (intento < 3) {
            return Mono.error(new RuntimeException("Servicio no disponible (intento " + intento + ")"));
        }
        return Mono.just("Datos del producto: " + productoId);
    }

    public static void main(String[] args) throws InterruptedException {

        // ─── 1. onErrorReturn: valor de fallback al ocurrir un error ──────
        System.out.println("=== ON_ERROR_RETURN: valor por defecto ante fallo ===");

        Flux.just("P001", "P002", "ERROR_ITEM", "P003")
            .map(id -> {
                if (id.startsWith("ERROR")) {
                    throw new IllegalArgumentException("ID inválido: " + id);
                }
                return "Producto: " + id;
            })
            .onErrorReturn("PRODUCTO_DESCONOCIDO")  // el Flux termina con este valor
            .subscribe(
                item -> System.out.println("  " + item),
                err  -> System.err.println("  Error no manejado: " + err.getMessage())
            );

        // ─── 2. onErrorContinue: continúa el Flux ignorando el elemento con error ──
        System.out.println("\n=== ON_ERROR_CONTINUE: ignorar elementos erróneos y continuar ===");

        Flux.just("P001", "P002", "ERROR_ITEM", "P003", "P004")
            .map(id -> {
                if (id.startsWith("ERROR")) {
                    throw new IllegalArgumentException("ID inválido: " + id);
                }
                return "Producto: " + id;
            })
            .onErrorContinue((err, item) ->
                System.out.println("  ⚠ Saltando elemento problemático: " + item + " → " + err.getMessage())
            )
            .subscribe(item -> System.out.println("  " + item));

        // ─── 3. onErrorResume: pipeline alternativo ante un error ─────────
        System.out.println("\n=== ON_ERROR_RESUME: pipeline de fallback ===");

        Mono<String> servicioFallido = Mono.error(new RuntimeException("BD no disponible"));
        Mono<String> cacheFallback   = Mono.just("Datos desde caché (fallback)");

        servicioFallido
            .onErrorResume(err -> {
                System.out.println("  Error detectado: " + err.getMessage());
                System.out.println("  Activando fallback hacia caché...");
                return cacheFallback;
            })
            .subscribe(dato -> System.out.println("  Resultado: " + dato));

        // ─── 4. retry: reintentar ante errores transitorios ───────────────
        System.out.println("\n=== RETRY: reintentar servicio inestable (máx 3 intentos) ===");

        intentos.set(0); // resetear contador

        servicioInestable("P001")
            .retryWhen(
                Retry.backoff(3, Duration.ofMillis(100))  // hasta 3 reintentos con backoff
                     .doBeforeRetry(signal ->
                         System.out.println("  Reintentando... intento #" + (signal.totalRetries() + 1))
                     )
            )
            .subscribe(
                dato -> System.out.println("  Éxito: " + dato),
                err  -> System.err.println("  Falló definitivamente: " + err.getMessage())
            );

        Thread.sleep(1000); // esperar reintentos con backoff

        // ─── 5. Combinación: retry + onErrorResume ────────────────────────
        System.out.println("\n=== RETRY + ON_ERROR_RESUME: máxima resiliencia ===");

        intentos.set(0);

        servicioInestable("P002")
            .retryWhen(Retry.fixedDelay(2, Duration.ofMillis(50)))
            .onErrorResume(err -> Mono.just("Valor por defecto tras agotar reintentos"))
            .subscribe(dato -> System.out.println("  Resultado final: " + dato));

        Thread.sleep(500);
    }
}
```

2. Ejecuta la clase.

#### Salida esperada
```
=== ON_ERROR_RETURN: valor por defecto ante fallo ===
  Producto: P001
  Producto: P002
  PRODUCTO_DESCONOCIDO

=== ON_ERROR_CONTINUE: ignorar elementos erróneos y continuar ===
  Producto: P001
  Producto: P002
  ⚠ Saltando elemento problemático: ERROR_ITEM → ID inválido: ERROR_ITEM
  Producto: P003
  Producto: P004

=== ON_ERROR_RESUME: pipeline de fallback ===
  Error detectado: BD no disponible
  Activando fallback hacia caché...
  Resultado: Datos desde caché (fallback)

=== RETRY: reintentar servicio inestable (máx 3 intentos) ===
    → Intento #1 para P001
  Reintentando... intento #1
    → Intento #2 para P001
  Reintentando... intento #2
    → Intento #3 para P001
  Éxito: Datos del producto: P001

=== RETRY + ON_ERROR_RESUME: máxima resiliencia ===
    → Intento #1 para P002
    → Intento #2 para P002
    → Intento #3 para P002
  Resultado final: Valor por defecto tras agotar reintentos
```

#### Verificación
- `onErrorReturn` termina el Flux con el valor de fallback; **no procesa P003** (el error termina el stream).
- `onErrorContinue` procesa P003 y P004 correctamente después de saltarse el elemento problemático.
- `retry` realiza exactamente 3 intentos con backoff antes de tener éxito en el intento #3.
- La combinación `retry + onErrorResume` agota los reintentos y luego usa el fallback.

---

## Validación y Pruebas

Crea la clase de test `PipelinesReactivosTest` en `src/test/java/com/curso/reactivo/lab01/`:

```java
package com.curso.reactivo.lab01;

import com.curso.reactivo.lab01.modelo.Producto;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

class PipelinesReactivosTest {

    @Test
    @DisplayName("Mono.just emite exactamente un elemento y completa")
    void monoJustEmiteUnElemento() {
        Mono<String> mono = Mono.just("Laptop");

        StepVerifier.create(mono)
            .expectNext("Laptop")
            .expectComplete()
            .verify();
    }

    @Test
    @DisplayName("Flux.fromIterable emite todos los elementos en orden")
    void fluxFromIterableEmiteEnOrden() {
        Flux<String> flux = Flux.just("A", "B", "C");

        StepVerifier.create(flux)
            .expectNext("A")
            .expectNext("B")
            .expectNext("C")
            .expectComplete()
            .verify();
    }

    @Test
    @DisplayName("filter elimina elementos que no cumplen la condición")
    void filterEliminaElementos() {
        Flux<Integer> numeros = Flux.range(1, 6)
                                    .filter(n -> n % 2 == 0);

        StepVerifier.create(numeros)
            .expectNext(2, 4, 6)
            .expectComplete()
            .verify();
    }

    @Test
    @DisplayName("map transforma cada elemento correctamente")
    void mapTransformaElementos() {
        Flux<String> nombres = Flux.just("laptop", "mouse")
                                   .map(String::toUpperCase);

        StepVerifier.create(nombres)
            .expectNext("LAPTOP", "MOUSE")
            .expectComplete()
            .verify();
    }

    @Test
    @DisplayName("onErrorReturn proporciona valor de fallback ante error")
    void onErrorReturnProporcionaFallback() {
        Mono<String> mono = Mono.<String>error(new RuntimeException("fallo"))
                                .onErrorReturn("FALLBACK");

        StepVerifier.create(mono)
            .expectNext("FALLBACK")
            .expectComplete()
            .verify();
    }

    @Test
    @DisplayName("retry reintenta el número especificado de veces")
    void retryReintentaCorrectamente() {
        AtomicInteger contador = new AtomicInteger(0);

        Mono<String> mono = Mono.defer(() -> {
            int intento = contador.incrementAndGet();
            if (intento < 3) {
                return Mono.error(new RuntimeException("Error temporal"));
            }
            return Mono.just("Éxito en intento " + intento);
        }).retryWhen(Retry.fixedDelay(3, Duration.ofMillis(10)));

        StepVerifier.create(mono)
            .expectNextMatches(s -> s.startsWith("Éxito"))
            .expectComplete()
            .verify(Duration.ofSeconds(5));
    }

    @Test
    @DisplayName("Mono.zip combina tres fuentes cuando todas completan")
    void monoZipCombinaTresFuentes() {
        Mono<String>  precio     = Mono.just("$100");
        Mono<Integer> stock      = Mono.just(50);
        Mono<String>  categoria  = Mono.just("ELECTRÓNICA");

        Mono<String> combinado = Mono.zip(precio, stock, categoria)
            .map(t -> t.getT1() + "|" + t.getT2() + "|" + t.getT3());

        StepVerifier.create(combinado)
            .expectNext("$100|50|ELECTRÓNICA")
            .expectComplete()
            .verify();
    }
}
```

Ejecuta los tests con:

```bash
mvn test -pl . -Dtest=PipelinesReactivosTest
```

**Resultado esperado:**
```
[INFO] Tests run: 6, Failures: 0, Errors: 0, Skipped: 0
[INFO] BUILD SUCCESS
```

---

## Resolución de Problemas

### Problema 1: El programa termina antes de que `Flux.interval` emita todos los elementos

**Síntoma:** La salida del ejercicio 5 (temporización) aparece incompleta o vacía cuando se usa `Flux.interval` sin `blockLast()` o sin `Thread.sleep()`.

**Causa:** `Flux.interval` usa el scheduler `Schedulers.parallel()` (hilos de fondo) por defecto. Cuando el método `main()` termina, la JVM finaliza todos los hilos de fondo antes de que emitan sus elementos, aunque el pipeline haya sido suscrito.

**Solución:** Agrega `Thread.sleep(duracionEsperada)` después de la suscripción, o usa `blockLast()` para bloquear el hilo `main()` hasta que el Flux complete. Ambas opciones son válidas **exclusivamente en `main()` o tests**:

```java
// Opción 1: blockLast() — bloquea hasta que el Flux complete
Flux.interval(Duration.ofMillis(200))
    .take(5)
    .doOnNext(t -> System.out.println("Tick: " + t))
    .blockLast();  // válido en main()

// Opción 2: Thread.sleep() — espera el tiempo estimado
Flux.interval(Duration.ofMillis(200))
    .take(5)
    .subscribe(t -> System.out.println("Tick: " + t));
Thread.sleep(1500); // 5 ticks × 200 ms + margen
```

---

### Problema 2: `StepVerifier` lanza `AssertionError: expectation "expectComplete" failed` en el test de `retry`

**Síntoma:** El test `retryReintentaCorrectamente` falla con un error similar a: `expectation "expectComplete" failed (timeout: 3s)` o el test nunca termina.

**Causa:** `Retry.fixedDelay()` usa `Schedulers.parallel()` para los delays entre reintentos. Si el `StepVerifier` se ejecuta con `verify()` sin un timeout explícito, puede bloquearse indefinidamente esperando señales de un scheduler virtual. Además, si el `AtomicInteger` no se inicializa correctamente entre tests, los reintentos pueden no alcanzar el umbral de éxito.

**Solución:** Siempre especifica un timeout en `verify()` para tests con delays reales, y usa `Duration.ofMillis(10)` como delay mínimo para mantener los tests rápidos:

```java
// Correcto: timeout explícito en verify()
StepVerifier.create(mono)
    .expectNextMatches(s -> s.startsWith("Éxito"))
    .expectComplete()
    .verify(Duration.ofSeconds(5));  // timeout de seguridad

// Si el test aún falla, verifica que el AtomicInteger sea local al test
// (no un campo estático compartido entre tests)
AtomicInteger contador = new AtomicInteger(0); // declarado dentro del test
```

---

## Limpieza

Este laboratorio no utiliza Docker ni bases de datos externas, por lo que la limpieza es mínima:

1. **Detener cualquier proceso Java en ejecución** (si dejaste algún `Flux.interval` sin terminar en modo debug):
   - En IntelliJ: haz clic en el botón **Stop** (cuadrado rojo) en la barra de ejecución.

2. **Limpiar artefactos de compilación** (opcional):
   ```bash
   mvn clean
   ```

3. **Guardar el proyecto** en tu repositorio Git para usarlo como referencia en los labs posteriores:
   ```bash
   git init
   git add .
   git commit -m "Lab 01-00-01: Primeros pipelines reactivos con Project Reactor"
   ```

> **Nota para labs siguientes:** El proyecto `reactor-lab01` es independiente y no es extendido directamente por el Lab 02. Sin embargo, los patrones aprendidos aquí (`Mono`, `Flux`, operadores, backpressure, manejo de errores) son la base de todos los labs posteriores. Conserva el código como referencia.

---

## Resumen

En esta práctica construiste una progresión completa de pipelines reactivos con Project Reactor:

| Ejercicio | Concepto clave | Operadores usados |
|-----------|---------------|-------------------|
| 1 | Mono y Flux básicos; `block()` vs `subscribe()` | `just`, `empty`, `fromIterable`, `subscribe`, `block` |
| 2 | Factories avanzadas para fuentes programáticas | `generate`, `create`, `range` |
| 3 | Transformación de datos en el pipeline | `map`, `flatMap`, `concatMap`, `filter` |
| 4 | Composición paralela de múltiples fuentes | `zip`, `merge`, `concat` |
| 5 | Temporización y streams en tiempo real | `delayElements`, `interval`, `take` |
| 6 | Backpressure: controlar la demanda | `limitRate`, `onBackpressureBuffer`, `onBackpressureDrop` |
| 7 | Resiliencia: recuperación de errores | `onErrorReturn`, `onErrorContinue`, `onErrorResume`, `retry` |

### Conceptos fundamentales reforzados

- **`block()` es un antipatrón en producción**: solo es aceptable en `main()` o tests. En un controller WebFlux o dentro de un pipeline reactivo, bloquea el event loop y anula todos los beneficios del modelo reactivo.
- **Nada ocurre hasta que hay un suscriptor**: los pipelines reactivos son *lazy* (perezosos). La cadena de operadores describe *qué* hacer, pero no se ejecuta hasta que `subscribe()` o `block()` son llamados.
- **`flatMap` vs `concatMap`**: `flatMap` maximiza el paralelismo (puede reordenar), `concatMap` preserva el orden (suscribe secuencialmente).
- **Backpressure es un mecanismo de protección**: evita que un productor rápido desborde a un suscriptor lento. Elige la estrategia según el caso de uso: buffer (tolera picos), drop (descarta exceso), limitRate (controla demanda).

### Recursos adicionales

- [Documentación oficial de Project Reactor — Referencia de Mono y Flux](https://projectreactor.io/docs/core/release/reference/)
- [Reactor 3 Reference Guide — Which operator do I need?](https://projectreactor.io/docs/core/release/reference/#which-operator)
- [Marble diagrams interactivos de operadores Reactor](https://rxmarbles.com/)
- [Baeldung — Guide to Reactor Core](https://www.baeldung.com/reactor-core)

---
