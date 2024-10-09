import { app, errorHandler } from 'mu';
import { CronJob } from 'cron';
import * as http from 'node:http';
import * as env from './env';
import {
  exportTaskByUuid,
  insertNewTask,
  isExportRunning,
  cleanup,
  getTasksThatCanBeRetried,
} from './lib/export-task';
import { waitForDatabase } from './database-utils';

/** Run on startup */
waitForDatabase().then(cleanup());

/** Schedule export cron job */
new CronJob(
  env.EXPORT_CRON_PATTERN,
  function () {
    console.log(`Export triggered by cron job at ${new Date().toISOString()}`);
    http
      .request({
        path: '/export-tasks',
        method: 'POST',
      })
      .end();
  },
  null,
  true,
);

new CronJob(
  env.RETRY_CRON_PATTERN,
  async () => {
    const retriableTasks = await getTasksThatCanBeRetried();
    for (const task of retriableTasks) {
      await task.retry();
    }
  },
  null,
  true,
);

/**
 * Triggers an async export task for the mandatendatabank and writes the data
 * dumps to files in /data/exports
 *
 * @return [202] if export started successfully. Location header contains an
 * endpoint to monitor the task status
 * @return [503] if an export task is already running
 */
app.post('/export-tasks', async function (req, res, next) {
  if (await isExportRunning()) return res.status(503).end();

  try {
    const task = await insertNewTask();

    // Don't await this call since the export is executed asynchronously:
    task.perform();

    return res.status(202).location(`/export-tasks/${task.id}`).end();
  } catch (e) {
    return next(new Error(e.message));
  }
});

/**
 * Get the status of a task
 *
 * @return [200] with task status object
 * @return [404] if task with given id cannot be found
 */
app.get('/export-tasks/:id', async function (req, res) {
  const taskId = req.params.id;
  const exportTask = await exportTaskByUuid(taskId);

  if (exportTask) {
    return res.send(exportTask.toJsonApi());
  } else {
    return res.status(404).end();
  }
});

app.use(errorHandler);
