import envvar from 'env-var';

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
  .default('/data/exports')
  .asString();
