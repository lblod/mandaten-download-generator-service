import fs from 'node:fs/promises';
import fsSync from 'node:fs';
import { querySudo } from '@lblod/mu-auth-sudo';
import * as http from 'node:http';
import FormData from 'form-data';
import * as env from '../../env';
import config from '../../config/type-export';

/**
 * Export resources of specific types with their configured optional and
 * required properties in Turtle format to a file. The exported types and
 * properties are defined in /config/type-export.json
 *
 * @param {string} file Absolute path of the file to export to (e.g.
 * /data/exports/mandaten.ttl)
 */
export default async function exportAsync(file) {
  const tmpFile = `${file}.tmp`;
  const prefixes = prefixStatements(config.prefixes);
  for (var typeConfig of config.types) {
    const count = await countForType(prefixes, typeConfig);
    console.log(`Exporting 0/${count} of ${typeConfig.type}`);

    let offset = 0;
    const query = `
      ${prefixes}
      CONSTRUCT {
        ${constructStatementsForType(typeConfig)}
      }
      WHERE {
        {
          SELECT DISTINCT ?resource WHERE {
            ${whereStatementsForType(typeConfig, false)}
          }
          ORDER BY ?resource
          LIMIT ${env.EXPORT_TTL_BATCH_SIZE}
          OFFSET %OFFSET
        }
        ${whereStatementsForType(typeConfig)}
      }`;

    while (offset < count) {
      await appendBatch(tmpFile, query, offset);
      offset = offset + env.EXPORT_TTL_BATCH_SIZE;
      console.log(
        `Constructed ${offset < count ? offset : count}/${count} of ${typeConfig.type}`,
      );
    }
  }
  await fs.rename(tmpFile, file);
}

// Private

async function countForType(prefixes, config) {
  const queryResult = await querySudo(`
    ${prefixes}
    SELECT (COUNT(DISTINCT(?resource)) as ?count)
    WHERE {
      ${whereStatementsForType(config, false)}
    }`);

  return parseInt(queryResult.results.bindings[0].count.value);
}

function appendBatch(file, query, offset = 0) {
  return new Promise((resolve, reject) => {
    const format = 'text/turtle';
    const url = new URL(env.MU_SPARQL_ENDPOINT);
    const fileStream = fsSync.createWriteStream(file, { flags: 'a' });
    const formData = new FormData();
    formData.append('format', format);
    formData.append('query', query.replace('%OFFSET', offset));
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
        res.pipe(fileStream);
        res.on('end', resolve);
      },
    );
    req.on('error', reject);
    formData.pipe(req);
  });
}

function prefixStatements(prefixes) {
  return Object.keys(prefixes)
    .map((prefix) => `PREFIX ${prefix}: <${prefixes[prefix]}>`)
    .join('\n');
}

function constructStatementsForType(config) {
  const construct = [];
  construct.push(`?resource a ${config.type} .`);
  construct.push(
    ...(config.requiredProperties || []).map(
      (prop) => `?resource ${prop} ${varName(prop)} .`,
    ),
  );
  construct.push('?resource ?optional_pred ?optional_obj .');
  return construct.join('\n');
}

function whereStatementsForType(config, includeOpt = true) {
  const where = [];
  where.push(`?resource a ${config.type} .`);
  where.push(
    ...(config.requiredProperties || []).map(
      (prop) => `?resource ${prop} ${varName(prop)} .`,
    ),
  );

  if (
    includeOpt &&
    config.optionalProperties &&
    config.optionalProperties.length
  ) {
    const optionalProps = [...config.optionalProperties, 'rdf:type'];
    where.push('?resource ?optional_pred ?optional_obj .');
    where.push(`FILTER (?optional_pred IN (${optionalProps.join(', ')}))`);
  }

  where.push(config.additionalFilter);
  return where.join('\n');
}

function varName(prop) {
  return `?${prop.replace(/:/g, '')}`;
}
