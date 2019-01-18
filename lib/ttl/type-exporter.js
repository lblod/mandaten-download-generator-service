import fs from 'fs-extra';
import request from 'request';
import { querySudo as query } from '../auth-sudo';

const batchSize = parseInt(process.env.EXPORT_TTL_BATCH_SIZE) || 1000;

/**
 * Export resources of specific types with their configured optional and required properties
 * in Turtle format to a file. The exported types and properties are defined in /config/type-export.json
 *
 * @param {string} file Absolute path of the file to export to (e.g. /data/exports/mandaten.ttl)
*/
async function exportAsync(file) {
  const tmpFile = `${file}.tmp`;
  const config = JSON.parse(fs.readFileSync(`/config/type-export.json`));
  const prefixes = prefixStatements(config.prefixes);
  for (var typeConfig of config.types) {
    const count = await countForType(prefixes, typeConfig);
    console.log(`Exporting 0/${count} of ${typeConfig.type}`);

    let offset = 0;
    const query = `${prefixes}
      CONSTRUCT {
        ${constructStatementsForType(typeConfig)}
      }
      WHERE {
        {
          SELECT DISTINCT ?resource WHERE {
            ${whereStatementsForType(typeConfig, false)}
          }
          LIMIT ${batchSize} OFFSET %OFFSET
        }
        ${whereStatementsForType(typeConfig)}
      }
    `;

    while (offset < count) {
      await appendBatch(tmpFile, query, offset);
      offset = offset + batchSize;
      console.log(`Constructed ${offset < count ? offset : count}/${count} of ${typeConfig.type}`);
    }
  };
  await fs.rename(tmpFile, file);
}

// private

async function countForType(prefixes, config) {
  const queryResult = await query(`${prefixes}
      SELECT (COUNT(DISTINCT(?resource)) as ?count)
      WHERE {
        ${whereStatementsForType(config, false)}
      }
    `);

  return parseInt(queryResult.results.bindings[0].count.value);
}

async function appendBatch(file, query, offset = 0, limit = 1000) {
  const format = 'text/turtle';
  const options = {
    method: 'POST',
    url: process.env.MU_SPARQL_ENDPOINT,
    headers: {
      'Accept': format
    },
    qs: {
      format: format,
      query: query.replace('%OFFSET', offset)
    }
  };

  return new Promise ( (resolve,reject) => {
    console.log(options.qs.query);
    const writer = fs.createWriteStream(file, { flags: 'a' });
    try {
      writer.on('finish', resolve);
      return request(options)
        .on('error', (error) => { reject(error); })
        .on('end', () => { writer.end("\n"); })
        .pipe(writer, { end: false });
    }
    catch(e) {
      writer.end();
      return reject(e);
    }
  });
}

function prefixStatements(prefixes) {
  return Object.keys(prefixes).map(function(prefix, index) {
    return `PREFIX ${prefix}: <${prefixes[prefix]}>`;
  }).join('\n');
}

function constructStatementsForType(config) {
  const construct = [];
  construct.push(`?resource a ${config.type}.`);
  construct.push(...(config.requiredProperties || []).map((prop) => `?resource ${prop} ${varName(prop)} .`));
  construct.push(...(config.optionalProperties || []).map((prop) => `?resource ${prop} ${varName(prop)} .`));
  return construct.join('\n');
}

function whereStatementsForType(config, includeOpt = true) {
  const where = [];
  where.push(`?resource a ${config.type}.`);
  where.push(... (config.requiredProperties || []).map((prop) => `?resource ${prop} ${varName(prop)} .`));

  if (includeOpt && config.optionalProperties && config.optionalProperties.length) {
    where.push('?resource ?optional_pred ?optional_obj.');
    where.push(`FILTER(?optional_pred IN (${config.optionalProperties.join(', ')}))`);
  }

  where.push(config.additionalFilter);
  return where.join('\n');
}

function varName(prop) {
  return `?${prop.replace(/:/g, '')}`;
}

export default exportAsync;
export { exportAsync };
