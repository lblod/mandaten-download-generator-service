import fs from 'node:fs/promises';
import fsSync from 'node:fs';
import * as http from 'node:http';
import FormData from 'form-data';
import * as env from '../../env';

/**
 * Export CSV to a file based on a SELECT-query defined in
 * /config/csv-export.sparql
 *
 * @param {string} file Absolute path of the file to export to (e.g.
 * /data/exports/mandaten.csv)
 */
export default async function exportAsync(file) {
  const tmpFile = `${file}.tmp`;
  const query = await fs.readFile(env.CSV_EXPORT_SPARQL_FILE);
  let offset = 0;
  let hasNext = true;

  while (hasNext) {
    hasNext = await appendBatch(
      tmpFile,
      query,
      offset,
      env.EXPORT_TTL_BATCH_SIZE,
      offset == 0,
    );
    offset = offset + env.EXPORT_TTL_BATCH_SIZE;
    console.log(`${offset} CSV records processed`);
    console.log(
      `Sleeping ${env.SLEEP_INTERVAL} ms before fetching the next batch.`,
    );
    await new Promise((p) => setTimeout(p, env.SLEEP_INTERVAL));
  }

  await fs.rename(tmpFile, file);
}

// Private

function appendBatch(
  file,
  query,
  offset = 0,
  limit = 1000,
  writeColumnHeader = false,
) {
  return new Promise((resolve, reject) => {
    const format = 'text/csv';
    const url = new URL(env.MU_SPARQL_ENDPOINT);
    let nbOfRecords = 0;
    let chunkCount = 0;
    const fileStream = fsSync.createWriteStream(file, { flags: 'a' });
    const formData = new FormData();
    formData.append('format', format);
    formData.append('query', `${query} LIMIT ${limit} OFFSET ${offset}`);
    const req = http.request(
      url,
      {
        method: 'POST',
        headers: {
          Accept: format,
          'Content-Length': formData.getBuffer().length,
          ...formData.getHeaders(),
        },
      },
      (res) => {
        if (res.statusCode !== 200)
          reject(
            new Error(`Error with the request: statuscode ${res.statusCode}`),
          );
        res.on('data', (chunk) => {
          let lines = chunk
            .toString()
            .split('\n')
            .map((e) => e.trim())
            .filter((e) => e.length > 0);
          if (chunkCount === 0 && !writeColumnHeader) {
            // Special: filter out the first line which is the header
            lines = lines.slice(1);
            fileStream.write(lines.join('\n'));
            nbOfRecords += lines.length;
          } else if (chunkCount === 0) {
            // Keep the header, but there is one record less than the length of the lines
            fileStream.write(chunk);
            nbOfRecords += lines.length - 1;
          } else {
            // All data, write directly, but still perform the count
            fileStream.write(chunk);
            nbOfRecords += lines.length;
          }
          chunkCount++;
        });
        res.on('end', () => {
          fileStream.end();
          return resolve(nbOfRecords > 0);
        });
      },
    );
    req.on('error', reject);
    formData.pipe(req);
  });
}
