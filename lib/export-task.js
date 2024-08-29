import {
  uuid,
  sparqlEscapeString,
  sparqlEscapeUri,
  sparqlEscapeInt,
  sparqlEscapeDateTime,
} from 'mu';
import exportCsv from './csv/sparql-exporter';
import exportTtl from './ttl/type-exporter';
import { insertNewExportFile } from './export-file';
import path from 'path';
import fs from 'node:fs/promises';
import { existsSync } from 'node:fs';
import { querySudo, updateSudo } from '@lblod/mu-auth-sudo';
import * as env from '../env';

const OUTPUT_DIR = path.join(env.FILE_OUTPUT_DIR, env.EXPORT_FILE_BASE);

fs.mkdirSync(OUTPUT_DIR, { recursive: true });

class ExportTask {
  constructor(content) {
    for (var key in content) this[key] = content[key];
  }

  /**
   * Retries the task
   *
   * @method retry
   */
  async retry() {
    const queryResult = await querySudo(`
      ${env.PREFIXES}

      SELECT ?retries WHERE {
        ${sparqlEscapeUri(this.uri)} export:numberOfRetries ?retries .
      }`);
    if (!queryResult.results.bindings[0]) {
      const retries = 0;
      await updateSudo(`
        ${env.PREFIXES}

        WITH ${sparqlEscapeUri(env.JOBS_GRAPH)}
        DELETE {
          ${sparqlEscapeUri(this.uri)} adms:status ?status .
        }
        INSERT {
          ${sparqlEscapeUri(this.uri)}
            export:numberOfRetries ${sparqlEscapeInt(retries + 1)} ;
            adms:status ${sparqlEscapeUri(env.STATUS_BUSY)} .
        } WHERE {
          ${sparqlEscapeUri(this.uri)} adms:status ?status .
        }`);
    } else {
      const retries = +queryResult.results.bindings[0].retries.value;
      await updateSudo(`
        ${env.PREFIXES}

        WITH ${sparqlEscapeUri(env.JOBS_GRAPH)}
        DELETE {
          ${sparqlEscapeUri(this.uri)}
            export:numberOfRetries ?retries ;
            adms:status ?status .
        }
        INSERT {
          ${sparqlEscapeUri(this.uri)}
            export:numberOfRetries ${sparqlEscapeInt(retries + 1)} ;
            adms:status ${sparqlEscapeUri(env.STATUS_BUSY)} .
        } WHERE {
          ${sparqlEscapeUri(this.uri)}
            export:numberOfRetries ?retries ;
            adms:status ?status .
        }`);
    }
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
      const filename = `${env.EXPORT_FILE_BASE}-${new Date().toISOString().replace(/-|T|Z|:|\./g, '')}`;

      const files = [];
      if (existsSync(env.CSV_EXPORT_SPARQL_FILE)) {
        const csvFile = path.join(env.FILE_OUTPUT_DIR, `${filename}.csv`);
        await exportCsv(csvFile);
        files.push(await insertNewExportFile(csvFile, 'text/csv'));
      } else {
        console.warn(
          `Not creating a CSV export: file ${sparqlFile} does not exist or no access to it`,
        );
        const ttlFile = path.join(env.FILE_OUTPUT_DIR, `${filename}.ttl`);
        await exportTtl(ttlFile);
        files.push(await insertNewExportFile(ttlFile, 'text/turtle'));
      }

      console.log(`Finish task ${this.id}`);
      await finishTask(this, files);
    } catch (err) {
      console.log(`Export failed: ${err}`);
      await finishTask(this, [], true);
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
          status: this.status,
          numberOfRetries: this.numberOfRetries,
        },
      },
    };
  }
}

/**
 * Insert a new export task
 *
 * @return {ExportTask} A new export task
 */
export async function insertNewTask() {
  const jobId = uuid();
  const jobUri = env.JOB_URI_PREFIX + `${jobId}`;
  const created = new Date();
  const createJobQuery = `
    ${env.PREFIXES}

    INSERT DATA {
      GRAPH ${sparqlEscapeUri(env.JOBS_GRAPH)} {
        ${sparqlEscapeUri(jobUri)}
          a ${sparqlEscapeUri(env.JOB_TYPE)} ;
          mu:uuid ${sparqlEscapeString(jobId)} ;
          dct:creator ${sparqlEscapeUri(env.JOB_CREATOR_URI)} ;
          adms:status ${sparqlEscapeUri(env.STATUS_BUSY)} ;
          dct:created ${sparqlEscapeDateTime(created)} ;
          dct:modified ${sparqlEscapeDateTime(created)} ;
          task:operation ${sparqlEscapeUri(env.JOB_OPERATION_URI)} . 
      }
    }`;

  await updateSudo(createJobQuery);

  const taskId = uuid();
  const taskUri = env.TASK_URI_PREFIX + `${taskId}`;
  const createTaskQuery = `
    ${env.PREFIXES}

    INSERT DATA {
      GRAPH ${sparqlEscapeUri(env.JOBS_GRAPH)} {
        ${sparqlEscapeUri(taskUri)}
          a ${sparqlEscapeUri(env.TASK_TYPE)} ;
          mu:uuid ${sparqlEscapeString(taskId)} ;
          adms:status ${sparqlEscapeUri(env.STATUS_BUSY)} ;
          dct:created ${sparqlEscapeDateTime(created)} ;
          dct:modified ${sparqlEscapeDateTime(created)} ;
          task:operation ${sparqlEscapeUri(env.TASK_OPERATION_URI)} ;
          task:index "0" ;
          dct:isPartOf ${sparqlEscapeUri(jobUri)} .
      }
    }`;

  await updateSudo(createTaskQuery);

  return new ExportTask({
    uri: taskUri,
    id: taskId,
    isPartOf: jobUri,
    status: env.STATUS_BUSY,
  });
}

/**
 * Get all failed tasks that can be retried
 *
 * @return {[ExportTask]} The failed tasks that can be retried
 */
export async function getTasksThatCanBeRetried() {
  const queryResult = await querySudo(`
      ${env.PREFIXES}

      SELECT * WHERE {
        ?uri
          a ${sparqlEscapeUri(env.TASK_TYPE)} ;
          dct:isPartOf ?jobUri ;
          mu:uuid ?uuid ;
          adms:status ${sparqlEscapeUri(env.STATUS_FAILED)} .
        OPTIONAL {
          ?uri export:numberOfRetries ?retries .
        }
        FILTER(!bound(?retries) || ?retries < ${sparqlEscapeInt(env.MAX_NUMBER_OF_RETRIES)})
    }`);
  const tasks = queryResult.results.bindings.map((result) => {
    return new ExportTask({
      uri: result.uri.value,
      id: result.uuid.value,
      isPartOf: result.jobUri.value,
      status: env.STATUS_FAILED,
    });
  });
  return tasks;
}

/**
 * Finish task with the given uuid
 *
 * @param {Object} exportTask
 * @param {boolean} failed whether the task failed to finish
 */
export async function finishTask(task, files = [], failed = false) {
  const status = failed ? env.STATUS_FAILED : env.STATUS_SUCCESS;

  if (files.length) {
    const containerId = uuid();
    const containerUri = env.CONTAINER_URI_PREFIX + containerId;

    const dataObjectTriples = files
      .map(
        (f) =>
          `${sparqlEscapeUri(containerUri)} task:hasFile ${sparqlEscapeUri(f.uri)} .`,
      )
      .join('\n');

    const insertContainerQuery = `
      ${env.PREFIXES}

      INSERT {
        GRAPH ?g {
          ${sparqlEscapeUri(containerUri)}
            a nfo:DataContainer ;
            mu:uuid ${sparqlEscapeString(containerId)} .
          ?task task:resultsContainer ${sparqlEscapeUri(containerUri)} .
          ${dataObjectTriples}
        }
      }
      WHERE {
        BIND(${sparqlEscapeUri(task.uri)} as ?task)
        GRAPH ?g {
          ?task a ${sparqlEscapeUri(env.TASK_TYPE)} .
        }
      }`;
    await updateSudo(insertContainerQuery);
  }

  const updateTaskQuery = `
    ${env.PREFIXES}

    DELETE {
      GRAPH ?g {
        ?task adms:status ?status .
      }
    }
    INSERT {
      GRAPH ?g {
        ?task adms:status ${sparqlEscapeUri(status)} .
      }
    }
    WHERE {
      BIND(${sparqlEscapeUri(task.uri)} as ?task)

      GRAPH ?g {
        ?task adms:status ?status .
      }
    }`;

  await updateSudo(updateTaskQuery);

  const updateJobQuery = `
    ${env.PREFIXES}

    DELETE {
      GRAPH ?g {
        ?job adms:status ?status .
      }
    }
    INSERT {
      GRAPH ?g {
        ?job adms:status ${sparqlEscapeUri(status)} .
      }
   }
   WHERE {
     BIND(${sparqlEscapeUri(task.isPartOf)} as ?job)

     GRAPH ?g {
       ?job adms:status ?status .
     }
   }`;

  await updateSudo(updateJobQuery);
}

/**
 * Cleanup ongoing tasks
 */
export async function cleanup() {
  //This detour to avoid sprintF issues
  const selectCleanupTasksQuery = `
    ${env.PREFIXES}

    SELECT ?task ?graph WHERE {
      BIND(${sparqlEscapeUri(env.STATUS_BUSY)} as ?status)
      GRAPH ?g {
        ?task
          a ${sparqlEscapeUri(env.TASK_TYPE)} ;
          adms:status ?status .
      }
    }`;

  let result = await querySudo(selectCleanupTasksQuery);

  if (result.results.bindings.length) {
    const updateTaskQuery = `
      ${env.PREFIXES}

      DELETE {
        GRAPH ?g {
          ?task adms:status ?status .
          ?job adms:status ?jobStatus .
        }
      }
      INSERT {
        GRAPH ?g {
          ?task adms:status ${sparqlEscapeUri(env.STATUS_CANCELED)} .
          ?jobs adms:status ${sparqlEscapeUri(env.STATUS_CANCELED)} .
        }
      }
      WHERE {
        BIND(${sparqlEscapeUri(env.STATUS_BUSY)} as ?status)

        GRAPH ?g {
          ?task
            a ${sparqlEscapeUri(env.TASK_TYPE)} ;
            adms:status ?status ;
            dct:isPartOf ?job .
          ?job adms:status ?jobStatus .
        }
      }`;

    await updateSudo(updateTaskQuery);
  }
}

/**
 * Get an export task by uuid
 *
 * @param {string} uuid uuid of the export task
 *
 * @return {ExportTask} Export task with the given uuid. Null if not found.
 */
export async function exportTaskByUuid(uuid) {
  const queryResult = await querySudo(`
    ${env.PREFIXES}

    SELECT * WHERE {
      GRAPH ?g {
        ?uri
          a ${sparqlEscapeUri(env.TASK_TYPE)} ;
          mu:uuid ${sparqlEscapeString(uuid)} ;
          adms:status ?status .
        OPTIONAL {
          ?uri export:numberOfRetries ?retries .
        }
      }
    }
    LIMIT 1`);

  if (queryResult.results.bindings.length) {
    const result = queryResult.results.bindings[0];
    return new ExportTask({
      uri: result.uri.value,
      id: uuid,
      status: result.status.value,
      numberOfRetries: result.retries ? result.retries.value : 0,
    });
  }
}

/**
 * Returns whether an export task is currently running
 * @return {boolean} Whether an export task is currently running
 */
export async function isExportRunning() {
  const queryResult = await querySudo(`
    ${env.PREFIXES}

    ASK {
      GRAPH ?g {
        ?uri
          a ${sparqlEscapeUri(env.TASK_TYPE)} ;
          task:operation ${sparqlEscapeUri(env.TASK_OPERATION_URI)} ;
          adms:status ${sparqlEscapeUri(env.STATUS_BUSY)} .
      }
    }`);
  return queryResult.boolean;
}
