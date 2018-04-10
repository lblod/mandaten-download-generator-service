import fs from 'fs-extra';
import request from 'request';
import eventStream from 'event-stream';

const batchSize = 1000;

/**
 * Export CSV to a file based on a SELECT-query defined in /config/csv-export.sparql
 *
 * @param {string} file Absolute path of the file to export to (e.g. /data/exports/mandaten.csv)
*/
async function exportAsync(file) {
  const tmpFile = `${file}.tmp`;
  const query = await fs.readFile(`/config/csv-export.sparql`);
  let offset = 0;
  
  let hasNext = true;
  while (hasNext) {
    hasNext = await appendBatch(tmpFile, query, offset, batchSize, offset == 0);
    offset = offset + batchSize;
    console.log(`${offset} CSV records processed`);
  }

  await fs.rename(tmpFile, file);
}

// private

async function appendBatch(file, query, offset = 0, limit = 1000, writeColumnHeader = false) {
  const format = 'text/csv';
  const options = {
    method: 'POST',
    url: process.env.MU_SPARQL_ENDPOINT,
    headers: {
      'Accept': format
    },
    qs: {
      format: format,
      query: `${query} LIMIT ${limit} OFFSET ${offset}`
    }
  };

  let lineNb = 0;
  let nbOfRecords = 0;
  await new Promise(resolve =>
                    request(options)
                    .on('error', (error) => { throw error; })
                    .pipe(eventStream.split())
                    .pipe(eventStream.map(function (line, callback) {
                      if (lineNb == 0 && !writeColumnHeader) {
                        callback(); // skip first line
                      } else if (!line.length) {
                        callback(); // skip empty lines
                      } else {
                        callback(null, `${line}\n`);
                        nbOfRecords++;
                      }
                      lineNb++;
                    }))
                    .pipe(fs.createWriteStream(file, { flags: 'a' }))
                    .on('finish', resolve));
  return nbOfRecords > 0;
}

export default exportAsync;
export { exportAsync };
