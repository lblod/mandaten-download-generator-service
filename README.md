# mandaten-download-generator-service

Microservice that generates the dump files (CSV, TTL) of mandatendatabank asynchronously. A cron job is embedded in the service to trigger an export at the preconfigured frequency. Exports are available in `/data/exports`. The export files are available in the triple store as resources of type `<http://mu.semte.ch/vocabularies/ext/export/Export>`.

## Installation
To add the service to your stack, add the following snippet to `docker-compose.yml`:
```
services:
  export:
    image: lblod/mandaten-download-generator-service:0.3.3
    volumes:
      - ./data/exports:/data/exports
      - ./config/export:/config
```

Don't forget to update the dispatcher configuration to route requests to the export service.

## Configuration
### CSV export
The SPARQL query to execute for the CSV export must be specified in `/config/csv-export.sparql`. Note that the variable names in the `SELECT` clause will be used as column headers in the export.

### TTL export
The Turtle export must be specified in `/config/type-export.json`. This JSON specifies a prefix mapping and a list of RDF types with a set of required and optional properties that must be exported per type. An additional filter for the `WHERE` clause can be specified per type.

E.g.
```
{
  "prefixes": {
    "mandaat": "http://data.vlaanderen.be/ns/mandaat#",
    "person": "http://www.w3.org/ns/person#",
    "foaf": "http://xmlns.com/foaf/0.1/"
  },
  "types": [
    {
      "type": "mandaat:Mandataris",
      "requiredProperties": [
        "mandaat:start",
        "mandaat:eind"
      ],
      "optionalProperties": [
        "mandaat:status"
      ],
      "additionalFilter": ""
    },
    {
      "type": "person:Person",
      "optionalProperties": [
        "foaf:name"
      ],
      "additionalFilter": ""
    }    
  ]
}
```

### Environment variables
The following environment variables can be configured:
* `EXPORT_CRON_PATTERN`: cron pattern to configure the frequency of the cron job. The pattern follows the format as specified in [node-cron](https://www.npmjs.com/package/cron#available-cron-patterns). Defaults to `0 0 */2 * * *`, run every 2 hours.
* `EXPORT_FILE_BASE`: base name of the export file. Defaults to `mandaten`. The export file will be named `{EXPORT_FILE_BASE}-{timestamp}.{csv|ttl}`.
* `EXPORT_TTL_BATCH_SIZE`: batch size used as `LIMIT` in the `CONSTRUCT` SPARQL queries per type. Defaults to `1000`. To have a complete export, make sure `EXPORT_TTL_BATCH_SIZE * number_of_matching_triples` doesn't exceed the maximum number of triples return by the database (e.g. `ResultSetMaxRows` in Virtuoso).
* `RETRY_CRON_PATTERN`: cron pattern to configure the frequency of the function that retries failed tasks. The pattern follows the format as specified in [node-cron](https://www.npmjs.com/package/cron#available-cron-patterns). Defaults to `0 */10 * * * *`, run every 10 minutes.
* `NUMBER_OF_RETRIES`: defined the number of times a task will be retried

## REST API
### POST /export-tasks
Trigger a new export asynchronously.

Returns `202 Accepted` if the export started successfully. The location response header contains an endpoint to monitor the task status.

Returns `503 Service Unavailable` if an export is already running.

### GET /export-tasks/:id
Get the status of an export task.

Returns `200 OK` with a task resource in the response body. Task status is one of `ongoing`, `done`, `cancelled` or `failed`.

## Development
Add the following snippet to your stack during development:
```
services:
  export:
    image: semtech/mu-javascript-template:1.3.4
    ports:
      - 8888:80
    environment:
      NODE_ENV: "development"
    volumes:
      - /path/to/your/code:/app/
      - ./data/exports:/data/exports
      - ./config/export:/config
```
