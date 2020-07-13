import { uuid, sparqlEscapeString, sparqlEscapeUri, sparqlEscapeInt } from 'mu';
import exportCsv from './csv/sparql-exporter';
import exportTtl from './ttl/type-exporter';
import { sparqlFile } from './csv/sparql-exporter';
import { insertNewExportFile } from './export-file';
import fs from 'fs-extra';
import { querySudo as query, updateSudo as update } from './auth-sudo';

const outputDir = '/data/exports';
const exportFileBase = process.env.EXPORT_FILE_BASE || 'mandaten';

class ExportTask {
  // uri: null;
  // id: null;
  // status: null;
  constructor(content) {
    for( var key in content )
      this[key] = content[key];
  }

  /**
   * Retries the task
   *
   * @method retry
   */
  async retry() {
    const queryResult = await query(`
      PREFIX export: <http://mu.semte.ch/vocabularies/ext/export/>
      SELECT ?retries WHERE {
        ${sparqlEscapeUri(this.uri)} export:numberOfRetries ?retries
      }
    `)
    let retries = 0;
    if(queryResult.results.bindings[0]) {
      retries = +queryResult.results.bindings[0].retries.value;
    }
    await update(`
      PREFIX export: <http://mu.semte.ch/vocabularies/ext/export/>
  
      WITH <${process.env.MU_APPLICATION_GRAPH}>
      DELETE {
        ${sparqlEscapeUri(this.uri)} export:numberOfRetries ?retries.
      }
      INSERT { 
        ${sparqlEscapeUri(this.uri)} export:numberOfRetries ${sparqlEscapeInt(retries+1)}.
      } WHERE {
        ${sparqlEscapeUri(this.uri)} export:numberOfRetries ?retries.
      }
    `);
    await this.perform();
  }

  /**
   * Exports to perform
   *
   * @method perform
   */
  async perform() {
    console.log(`Start task ${this.id}`);
    try {
      const filename = `${exportFileBase}-${new Date().toISOString().replace(/-|T|Z|:|\./g, '')}`;

      if (fs.existsSync(sparqlFile)) {
        const csvFile = `${outputDir}/${filename}.csv`;
        await exportCsv(csvFile);
        await insertNewExportFile(csvFile, 'text/csv');
      }
      else {
        console.warn(`Not creating a CSV export: file ${sparqlFile} does not exist`);
      }
      const ttlFile = `${outputDir}/${filename}.ttl`;
      await exportTtl(ttlFile);
      await insertNewExportFile(ttlFile, 'text/turtle');

      console.log(`Finish task ${this.id}`);
      await finishTask(this.id);
    } catch(err) {
      console.log(`Export failed: ${err}`);
      await finishTask(this.id, true);
    }
  }

  /**
   * Wrap export-task in a JSONAPI compliant object
   *
   * @method toJsonApi
   * @return {Object} JSONAPI compliant wrapper for the export-task
   */
  toJsonApi() {
    return {
      data: {
        type: 'export-tasks',
        id: this.id,
        attributes: {
          uri: this.uri,
          status: this.status
        }
      }
    };
  }
  
}

/**
 * Insert a new export task
 *
 * @return {ExportTask} A new export task
 */
async function insertNewTask() {
  const taskId = uuid();
  const taskUri = `http://mu-exporter/tasks/${taskId}`;
  await update(
    `PREFIX export: <http://mu.semte.ch/vocabularies/ext/export/>
     PREFIX mu: <http://mu.semte.ch/vocabularies/core/>

     INSERT DATA { 
       GRAPH <${process.env.MU_APPLICATION_GRAPH}> {
           ${sparqlEscapeUri(taskUri)} a export:Task ; 
                mu:uuid ${sparqlEscapeString(taskId)} ;
                export:status "ongoing".
       }
     }`);

  return new ExportTask({
    uri: taskUri,
    id: taskId,
    status: "ongoing"
  });
}

/**
 * Get all failed tasks that can be retried
 *
 * @return {[ExportTask]} The failed tasks that can be retried
 */
async function getTasksThatCanBeRetried() {
  const queryResult = await query(`
    PREFIX export: <http://mu.semte.ch/vocabularies/ext/export/>
    PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
    SELECT * WHERE {
      ?uri a export:Task;
        mu:uuid ?uuid;
        export:status "failed";
        export:numberOfRetries ?retries.
      FILTER(?retries < 3)
    }
  `)
  const tasks = queryResult.results.bindings.map((result) => {
    return new ExportTask({
      uri: result.uri.value,
      id: result.uuid.value,
      status: "failed"
    });
  })
  return tasks;
}

/**
 * Finish task with the given uuid
 *
 * @param {string} uuid uuid of the export task
 * @param {boolean} failed whether the task failed to finish
 */ 
async function finishTask(uuid, failed = false) {
  const status = failed ? "failed" : "done";
  await update(
    `PREFIX export: <http://mu.semte.ch/vocabularies/ext/export/>
     PREFIX mu: <http://mu.semte.ch/vocabularies/core/>

     WITH <${process.env.MU_APPLICATION_GRAPH}>
     DELETE {
       ?s export:status ?status .
     }
     INSERT { 
       ?s export:status ${sparqlEscapeString(status)} .
     } WHERE {
       ?s a export:Task ; 
            mu:uuid ${sparqlEscapeString(uuid)} ;
            export:status ?status .
     }`);
}

/**
 * Cleanup ongoing tasks
*/
async function cleanup() {
  await update(
    `PREFIX export: <http://mu.semte.ch/vocabularies/ext/export/>
     PREFIX mu: <http://mu.semte.ch/vocabularies/core/>

     WITH <${process.env.MU_APPLICATION_GRAPH}>
     DELETE {
       ?s export:status ?status .
     }
     INSERT { 
       ?s export:status "cancelled" .
     } WHERE {
       ?s a export:Task ; 
            export:status ?status .

       FILTER(?status = "ongoing")
     }`);
}

/**
 * Get an export task by uuid
 *
 * @param {string} uuid uuid of the export task
 *
 * @return {ExportTask} Export task with the given uuid. Null if not found.
*/
async function exportTaskByUuid(uuid) {
  const queryResult = await query(
    `PREFIX export: <http://mu.semte.ch/vocabularies/ext/export/>
     PREFIX mu: <http://mu.semte.ch/vocabularies/core/>

     SELECT * 
     WHERE { 
       GRAPH <${process.env.MU_APPLICATION_GRAPH}> {
         ?uri a export:Task ; 
              mu:uuid ${sparqlEscapeString(uuid)} ;
              export:status ?status .
       }
     }`);

  if (queryResult.results.bindings.length) {
    const result = queryResult.results.bindings[0];
    return new ExportTask({
      uri: result.uri.value,
      id: uuid,
      status: result.status.value
    });
  } else {
    return null;
  }
}

/**
 * Returns whether an export task is currently running
 * @return {boolean} Whether an export task is currently running
*/
async function isExportRunning() {
  const queryResult = await query(
    `PREFIX export: <http://mu.semte.ch/vocabularies/ext/export/>
     PREFIX mu: <http://mu.semte.ch/vocabularies/core/>

     ASK { 
       GRAPH <${process.env.MU_APPLICATION_GRAPH}> {
         ?uri a export:Task ; 
              export:status "ongoing" .
       }
     }`);
  
  return queryResult.boolean;
}

export default ExportTask;
export {
  insertNewTask,
  finishTask,
  exportTaskByUuid,
  isExportRunning,
  cleanup,
  getTasksThatCanBeRetried
};
