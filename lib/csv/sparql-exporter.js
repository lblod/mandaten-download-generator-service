import fs from 'node:fs/promises';
import fetch from 'node-fetch';
import * as env from '../../env';

const sparqlFile = '/config/csv-export.sparql';
/**
 * Export CSV to a file based on a SELECT-query defined in /config/csv-export.sparql
 *
 * @param {string} file Absolute path of the file to export to (e.g. /data/exports/mandaten.csv)
 */
async function exportAsync(file) {
  const tmpFile = `${file}.tmp`;

  const query = await fs.readFile(sparqlFile);
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
  }

  await fs.rename(tmpFile, file);
}

// private

async function appendBatch(
  file,
  query,
  offset = 0,
  limit = 1000,
  writeColumnHeader = false,
) {
  const format = 'text/csv';
  let nbOfRecords = 0;
  const url = new URL(env.MU_SPARQL_ENDPOINT);
  const params = {
    format: format,
    query: `${query} LIMIT ${limit} OFFSET ${offset}`,
  };
  Object.keys(params).forEach((key) =>
    url.searchParams.append(key, params[key]),
  );
  const result = await fetch(url, {
    method: 'POST',
    headers: {
      Accept: format,
    },
  });
  const resultText = await result.text();
  if (result.status !== 200) {
    throw new Error(`Error with the request: ${resultText}`);
  }
  const proccessedText = [];
  resultText.split('\n').forEach((line, index) => {
    if (index == 0 && !writeColumnHeader) {
      return;
    } else if (!line.length) {
      return;
    } else {
      proccessedText.push(line);
      nbOfRecords++;
    }
  });

  await fs.appendFile(file, proccessedText.join('\n'));
  return nbOfRecords > 0;
}

export default exportAsync;
export { exportAsync, sparqlFile };
