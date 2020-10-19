# QueryER
## Usage

QueryER was written in Java 8 utilizing Apache Calcite.
To create the jar file, run: **`mvn clean compile assembly:single`**
To run the jar file, copy and paste the following command to the console:

**`java -DApp.config.location=config.properties Dlog4j.configuration=file:resources/log4j.properties -jar queryER-1.0-jar-with-dependencies.jar`**

### Configuration
In the default configuration the framework asks the user for queries, and does not need any further configuration.  The framework is configured through the **config.properties** file which has to be provided with the following properties:<br>
**schema.name**:{String} The schema for the queries. (Default = all)
**calcite.connection**:{String} The default calcite connection, connects to model.json.

For debugging and experimental purposes, the following can also be provided.<br>
**query.runs**{Integer}
**query.filepath=resources/tests/test_synth.sql** {Path to an sql file, if not provided the framework will ask the user for a query input}
**ground_truth.calculate**:{Boolean} Whether to calculate Ground Truth and find the Pair Completeness. Used only for SP queries.

### Queries
The queries that were used for the experimental evaluation can be found in the queries.sql file.


### Datasets
