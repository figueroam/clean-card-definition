# Clean Card Definition Table

App para eliminar los registros repetidos en la tabla de card-definitions de la base binapi.


------------

#### ¿Cómo funciona?

Se considera que hay registros repetidos, cuando se encuentran más de 1 registro que tenga los mismos campos isuer-id, brand-id, segment-id y card-type-id.

###### El algoritmo para realizar esta limpieza consta de los siguientes pasos:
1. Se recorre la tabla card-definitions armando un mapa en el cual la key del mapa está compuesta por una concatenación de "isuerId-brandId-cardTypeId-segmentId". Si la key no se encuentra en el mapa, se agrega de forma que el mapa queda:
`<key=keyCompuesta, value= <cardDefinitionId, []>`
Esto quiere decir que la primera que vez que se pase por esa key, tendremos una referencia al id de ese cardDefinition, en las próximas iteraciones iremos agregando a la lista los cardDefinitionIds que tengan la misma key. Y nos terminará quedando el id original y su lista de repetidos.
2. Se eliminaran del mapa aquellas keys que no posean repetidos.
3. Por cada key se procede a hacer dos cosas:
	1.  Cambiar los binSettings que posean una referencia a los cardDefinitionsIdsRepetidos, hacia el cardDefinitionOriginal
	2. Eliminar los CardDefinitionsRepetidos.

#### ¿Cómo probarlo?
Clonar el proyecto y cambiar los parametros de configuracion hacia la base de datos que se encuentran en el path "clean-card-definition/src/main/resources/application.properties"

**spring.datasource.url=jdbc:mysql://localhost:3306/binapi
spring.datasource.username=binapi
spring.datasource.password=binapi_dev**

Luego hacer un Request Post a: localhost:8080/clean-card-definition

El proceso tarda aproximadamente 32 minutos y podrán observar el avance en los logs de la aplicación.


#### ¿Cómo verifico el funcionamiento"

Antes de correr la aplicación se puede comprobar que la tabla posee registros repetidos agrupándolos y ejecutando siguiente query:
`SELECT issuer_id,card_type_id,brand_id,segment_id,count(1) cant FROM card_definitions GROUP BY issuer_id,card_type_id,brand_id,segment_id ORDER BY cant DESC;
`

Luego de correrlo si se vuelve a ejecutar la misma query, se podrá observar que no quedan registros con campos repetidos, mirando el último capo del output de la misma.


