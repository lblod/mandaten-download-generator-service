import envvar from 'env-var';

// Environment variables

export const EXPORT_CRON_PATTERN = envvar
  .get('EXPORT_CRON_PATTERN')
  .required()
  .example('0 0 0 * * *')
  .default('0 0 */2 * * *')
  .asString();

export const RETRY_CRON_PATTERN = envvar
  .get('RETRY_CRON_PATTERN')
  .required()
  .example('0 0 0 * * *')
  .default('*/10 * * * *')
  .asString();

export const MU_SPARQL_ENDPOINT = envvar
  .get('MU_SPARQL_ENDPOINT')
  .required()
  .example('http://virtuoso:8890/sparql')
  .default('http://database:8890/sparql')
  .asUrlString();

export const EXPORT_TTL_BATCH_SIZE = envvar
  .get('EXPORT_TTL_BATCH_SIZE')
  .default('1000')
  .asIntPositive();

export const MAX_NUMBER_OF_RETRIES = envvar
  .get('NUMBER_OF_RETRIES')
  .default('3')
  .asIntPositive();

export const MU_APPLICATION_GRAPH = envvar
  .get('MU_APPLICATION_GRAPH')
  .required()
  .example('http://mu-semtech/graphs/application')
  .asUrlString();

export const EXPORT_FILE_BASE = envvar
  .get('EXPORT_FILE_BASE')
  .required()
  .default('mandaten')
  .asString();

export const FILE_OUTPUT_DIR = envvar
  .get('FILE_OUTPUT_DIR')
  .required()
  .default('/share/exports')
  .asString();

export const JOBS_GRAPH = envvar
  .get('JOBS_GRAPH')
  .required()
  .default('http://mu.semte.ch/graphs/system/jobs')
  .asUrlString();

export const FILES_GRAPH = envvar
  .get('FILES_GRAPH')
  .required()
  .default('http://mu.semte.ch/graphs/system/jobs')
  .asUrlString();

export const EXPORT_CLASSIFICATION_URI = envvar
  .get('EXPORT_CLASSIFICATION_URI')
  .required()
  .default('http://redpencil.data.gift/id/exports/concept/GenericExport')
  .asUrlString();

export const TASK_OPERATION_URI = envvar
  .get('TASK_OPERATION_URI')
  .required()
  .default(
    'http://lblod.data.gift/id/jobs/concept/TaskOperation/exportMandatarissen',
  )
  .asUrlString();

export const CSV_EXPORT_SPARQL_FILE = envvar
  .get('CSV_EXPORT_SPARQL_FILE ')
  .default('/config/csv-export.sparql')
  .asString();

// Constants

export const STATUS_BUSY =
  'http://redpencil.data.gift/id/concept/JobStatus/busy';
export const STATUS_SCHEDULED =
  'http://redpencil.data.gift/id/concept/JobStatus/scheduled';
export const STATUS_SUCCESS =
  'http://redpencil.data.gift/id/concept/JobStatus/success';
export const STATUS_FAILED =
  'http://redpencil.data.gift/id/concept/JobStatus/failed';
export const STATUS_CANCELED =
  'http://redpencil.data.gift/id/concept/JobStatus/canceled';

export const JOB_OPERATION_URI =
  'http://redpencil.data.gift/id/jobs/concept/JobOperation/DownloadGeneration';
export const JOB_TYPE = 'http://vocab.deri.ie/cogs#Job';
export const TASK_TYPE = 'http://redpencil.data.gift/vocabularies/tasks/Task';
export const EXPORT_TYPE =
  'http://redpencil.data.gift/vocabularies/exports/Export';
export const ERROR_TYPE = 'http://open-services.net/ns/core#Error';

export const PREFIXES = `
  PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
  PREFIX task: <http://redpencil.data.gift/vocabularies/tasks/>
  PREFIX dct: <http://purl.org/dc/terms/>
  PREFIX prov: <http://www.w3.org/ns/prov#>
  PREFIX nie: <http://www.semanticdesktop.org/ontologies/2007/01/19/nie#>
  PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
  PREFIX oslc: <http://open-services.net/ns/core#>
  PREFIX cogs: <http://vocab.deri.ie/cogs#>
  PREFIX adms: <http://www.w3.org/ns/adms#>
  PREFIX export: <http://redpencil.data.gift/vocabularies/exports/>
  PREFIX nfo: <http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#>
  PREFIX dbpedia: <http://dbpedia.org/ontology/>
`;

export const TASK_URI_PREFIX = 'http://redpencil.data.gift/id/task/';
export const JOB_URI_PREFIX = 'http://redpencil.data.gift/id/job/';
export const CONTAINER_URI_PREFIX =
  'http://redpencil.data.gift/id/dataContainers/';
export const ERROR_URI_PREFIX = 'http://redpencil.data.gift/id/jobs/error/';
export const JOB_CREATOR_URI =
  'http://lblod.data.gift/services/DownloadGeneratorService';
