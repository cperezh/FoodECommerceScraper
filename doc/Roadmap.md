Problema secundario:
Quiero poder reprocesaer los datos de la tabla de hechos, sin generaer duplicados,
para poder introducir nuevas métricas y que se puedan recalcular a pasado.
También quiero poder reprocesar los datos si hay errores.

Hipótesis
Si hago cambios en la tabla de hechos, que necesiten una recarga de datos, necesito que no se generen duplicados.
Tengo que implementar un patron de Delete, Insert o Merge.

El problema de los Merge es que no controlas la volumetría de cambios que se va a ejecutar. En tablas grandes
(Como las de hechos) puede ser un problema. Mejor implementar un sistema de recarga controlada, por particiones.

Problema mayor: 
Quiero poder recargar cualquier tabla, sin generar duplicados.

### Particionar tabla de hechos

Yo como developer, 
quiero particionar la tabla de hechos
para comprobar como se comporta a nivel de ficheros
y mejorar el performance