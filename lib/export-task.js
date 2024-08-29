import {
  uuid,
  sparqlEscapeString,
  sparqlEscapeUri,
  sparqlEscapeInt,
  sparqlEscapeDateTime,
} from 'mu';
import exportCsv from './csv/sparql-exporter';
import exportTtl from './ttl/type-exporter';
import { sparqlFile } from './csv/sparql-exporter';
import { insertNewExportFile } from './export-file';
import path from 'path';
import fs from 'node:fs/promises';
import { querySudo as query, updateSudo as update } from './auth-sudo';
import * as env from '../env';

const numberOfRetries = process.env.NUMBER_OF_RETRIES || 3;
const exportFileBase = process.env.EXPORT_FILE_BASE || 'mandaten';
const outputDir = `/share/exports/${exportFileBase}`;

fs.mkdirSync(outputDir, { recursive: true });

import {
  PREFIXES,
  JOB_TYPE,
  TASK_TYPE,
  JOBS_GRAPH,
  STATUS_BUSY,
  STATUS_FAILED,
  STATUS_SUCCESS,
  STATUS_CANCELED,
  TASK_URI_PREFIX,
  JOB_URI_PREFIX,
  JOB_CREATOR_URI,
  JOB_OPERATION_URI,
  TASK_OPERATION_URI,
  CONTAINER_URI_PREFIX,
} from '../constants';

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
    const queryResult = await query(`
      ${PREFIXES}

      SELECT ?retries WHERE {
        ${sparqlEscapeUri(this.uri)} export:numberOfRetries ?retries .
      }`);
    if (!queryResult.results.bindings[0]) {
      const retries = 0;
      await update(`
        ${PREFIXES}

        WITH ${sparqlEscapeUri(JOBS_GRAPH)}
        DELETE {
          ${sparqlEscapeUri(this.uri)} adms:status ?status.
        }
        INSERT {
          ${sparqlEscapeUri(this.uri)} export:numberOfRetries ${sparqlEscapeInt(retries + 1)};
                                       adms:status ${sparqlEscapeUri(STATUS_BUSY)} .
        } WHERE {
          ${sparqlEscapeUri(this.uri)} adms:status ?status .
        }
      `);
    } else {
      const retries = +queryResult.results.bindings[0].retries.value;
      await update(`
        ${PREFIXES}

        WITH ${sparqlEscapeUri(JOBS_GRAPH)}
        DELETE {
          ${sparqlEscapeUri(this.uri)} export:numberOfRetries ?retries;
                                       adms:status ?status.
        }
        INSERT {
          ${sparqlEscapeUri(this.uri)} export:numberOfRetries ${sparqlEscapeInt(retries + 1)};
                                       adms:status ${sparqlEscapeUri(STATUS_BUSY)} .
        } WHERE {
          ${sparqlEscapeUri(this.uri)} export:numberOfRetries ?retries;
                                       adms:status ?status .
        }
        INSERT {
          ${sparqlEscapeUri(this.uri)}
            export:numberOfRetries ${sparqlEscapeInt(retries + 1)} ;
            export:status "ongoing" .
        } WHERE {
          ${sparqlEscapeUri(this.uri)}
            export:numberOfRetries ?retries ;
            export:status ?status .
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
      try {
        await fs.access(sparqlFile, fs.constants.R_OK); // Rejects on error
        const csvFile = path.join(env.FILE_OUTPUT_DIR, `${filename}.csv`);
        await exportCsv(csvFile);
        files.push(await insertNewExportFile(csvFile, 'text/csv'));
      } catch {
        console.warn(
          `Not creating a CSV export: file ${sparqlFile} does not exist or no access to it`,
        );
      }
      const ttlFile = path.join(env.FILE_OUTPUT_DIR, `${filename}.ttl`);
      await exportTtl(ttlFile);
      files.push(await insertNewExportFile(ttlFile, 'text/turtle'));

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
async function insertNewTask() {
  const jobId = uuid();
  const jobUri = JOB_URI_PREFIX + `${jobId}`;
  const created = new Date();
  const createJobQuery = `
    ${PREFIXES}

    INSERT DATA {
      GRAPH ${sparqlEscapeUri(JOBS_GRAPH)}{
        ${sparqlEscapeUri(jobUri)} a ${sparqlEscapeUri(JOB_TYPE)};
                                   mu:uuid ${sparqlEscapeString(jobId)};
                                   dct:creator ${sparqlEscapeUri(JOB_CREATOR_URI)};
                                   adms:status ${sparqlEscapeUri(STATUS_BUSY)};
                                   dct:created ${sparqlEscapeDateTime(created)};
                                   dct:modified ${sparqlEscapeDateTime(created)};
                                   task:operation ${sparqlEscapeUri(JOB_OPERATION_URI)}.
      }
    }
  `;

  await update(createJobQuery);

  const taskId = uuid();
  const taskUri = TASK_URI_PREFIX + `${taskId}`;
  const createTaskQuery = `
    ${PREFIXES}

    INSERT DATA {
     GRAPH ${sparqlEscapeUri(JOBS_GRAPH)} {
         ${sparqlEscapeUri(taskUri)} a ${sparqlEscapeUri(TASK_TYPE)};
                                  mu:uuid ${sparqlEscapeString(taskId)};
                                  adms:status ${sparqlEscapeUri(STATUS_BUSY)};
                                  dct:created ${sparqlEscapeDateTime(created)};
                                  dct:modified ${sparqlEscapeDateTime(created)};
                                  task:operation ${sparqlEscapeUri(TASK_OPERATION_URI)};
                                  task:index "0";
                                  dct:isPartOf ${sparqlEscapeUri(jobUri)}.
      }
    }`;

  await update(createTaskQuery);

  return new ExportTask({
    uri: taskUri,
    id: taskId,
    isPartOf: jobUri,
    status: STATUS_BUSY,
  });
}

/**
 * Get all failed tasks that can be retried
 *
 * @return {[ExportTask]} The failed tasks that can be retried
 */
async function getTasksThatCanBeRetried() {
  const queryResult = await query(`
      ${PREFIXES}

      SELECT * WHERE {
        ?uri a ${sparqlEscapeUri(TASK_TYPE)};
             dct:isPartOf ?jobUri;
             mu:uuid ?uuid;
             adms:status ${sparqlEscapeUri(STATUS_FAILED)}.

        OPTIONAL {
          ?uri export:numberOfRetries ?retries.
        }

        FILTER(!bound(?retries) || ?retries < ${sparqlEscapeInt(numberOfRetries)})
    }
  `);
  const tasks = queryResult.results.bindings.map((result) => {
    return new ExportTask({
      uri: result.uri.value,
      id: result.uuid.value,
      isPartOf: result.jobUri.value,
      status: STATUS_FAILED,
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
async function finishTask(task, files = [], failed = false) {
  const status = failed ? STATUS_FAILED : STATUS_SUCCESS;

  if (files.length) {
    const containerId = uuid();
    const containerUri = CONTAINER_URI_PREFIX + containerId;

    const dataObjectTriples = files
      .map(
        (f) =>
          `${sparqlEscapeUri(containerUri)} task:hasFile ${sparqlEscapeUri(f.uri)} .`,
      )
      .join('\n');

    const insertContainerQuery = `
       ${PREFIXES}

       INSERT {
        GRAPH ?g {
          ${sparqlEscapeUri(containerUri)} a nfo:DataContainer .
          ${sparqlEscapeUri(containerUri)} mu:uuid ${sparqlEscapeString(containerId)} .
          ?task task:resultsContainer ${sparqlEscapeUri(containerUri)} .
          ${dataObjectTriples}
        }
      }
      WHERE {
        BIND(${sparqlEscapeUri(task.uri)} as ?task)
        GRAPH ?g {
           ?task a ${sparqlEscapeUri(TASK_TYPE)}.
        }
      }
    `;
    await update(insertContainerQuery);
  }

  const updateTaskQuery = `
    ${PREFIXES}

    DELETE {
      GRAPH ?g {
       ?task adms:status ?status.
      }
    }
    INSERT {
      GRAPH ?g {
       ?task adms:status ${sparqlEscapeUri(status)}.
      }
   }
   WHERE {
      BIND(${sparqlEscapeUri(task.uri)} as ?task)

      GRAPH ?g {
       ?task adms:status ?status.
     }
   }`;

  await update(updateTaskQuery);

  const updateJobQuery = `
    ${PREFIXES}

    DELETE {
      GRAPH ?g {
       ?job adms:status ?status.
      }
    }
    INSERT {
      GRAPH ?g {
       ?job adms:status ${sparqlEscapeUri(status)}.
      }
   }
   WHERE {
      BIND(${sparqlEscapeUri(task.isPartOf)} as ?job)

      GRAPH ?g {
       ?job adms:status ?status.
     }
   }`;

  await update(updateJobQuery);
}

/**
 * Cleanup ongoing tasks
 */
async function cleanup() {
  //This detour to avoid sprintF issues
  const selectCleanupTasksQuery = `
    ${PREFIXES}

    SELECT ?task ?graph WHERE {
      BIND(${sparqlEscapeUri(STATUS_BUSY)} as ?status)
      GRAPH ?g {
        ?task a ${sparqlEscapeUri(TASK_TYPE)}.
        ?task adms:status ?status.
      }
    }
  `;

  let result = await query(selectCleanupTasksQuery);

  if (!result.results.bindings.length) {
    return;
  } else {
    const updateTaskQuery = `
      ${PREFIXES}

      DELETE {
        GRAPH ?g {
         ?task adms:status ?status.
         ?job adms:status ?jobStatus.
        }
      }
      INSERT {
        GRAPH ?g {
         ?task adms:status ${sparqlEscapeUri(STATUS_CANCELED)}.
         ?jobs adms:status ${sparqlEscapeUri(STATUS_CANCELED)}.
        }
     }
     WHERE {
        BIND(${sparqlEscapeUri(STATUS_BUSY)} as ?status)

        GRAPH ?g {
         ?task a ${sparqlEscapeUri(TASK_TYPE)}.
         ?task adms:status ?status.
         ?task dct:isPartOf ?job.
         ?job adms:status ?jobStatus.
       }

     }`;

    await update(updateTaskQuery);
  }
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
    `
     ${PREFIXES}

     SELECT * WHERE {
       GRAPH ?g {
         ?uri a ${sparqlEscapeUri(TASK_TYPE)} ;
              mu:uuid ${sparqlEscapeString(uuid)} ;
              adms:status ?status .
        OPTIONAL {
          ?uri export:numberOfRetries ?retries .
        }
      }
    }`,
  );

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
async function isExportRunning() {
  const queryResult = await query(
    `${PREFIXES}

     ASK {
       GRAPH ?g {
         ?uri a ${sparqlEscapeUri(TASK_TYPE)} ;
              adms:status ${sparqlEscapeUri(STATUS_BUSY)} .
       }
     }`,
  );

  return queryResult.boolean;
}

export default ExportTask;
export {
  insertNewTask,
  finishTask,
  exportTaskByUuid,
  isExportRunning,
  cleanup,
  getTasksThatCanBeRetried,
};
