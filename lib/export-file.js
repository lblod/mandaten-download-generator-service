import path from 'path';
import fs from 'node:fs/promises';
import {
  uuid,
  sparqlEscapeString,
  sparqlEscapeUri,
  sparqlEscapeInt,
  sparqlEscapeDateTime,
} from 'mu';
import { updateSudo } from '@lblod/mu-auth-sudo';
import * as env from '../env';

class ExportFile {
  // uri: null;
  // created: null;
  // format: null;
  // size: null;
  constructor(content) {
    for (var key in content) this[key] = content[key];
  }
}

/**
 * Insert (the metadata of) a new export file
 *
 * @param {string} filename Name of the export file
 * @param {string} format MIME type of the export file
 * @param {int} size Filesize in bytes
 *
 * @return {Export} A new export file
 */
async function insertNewExportFile(file, format) {
  const exportId = uuid();
  const exportUri = `http://mu-exporter/exports/${exportId}`;
  const created = new Date();
  const filename = path.basename(file);
  const extension = path.extname(file);
  const stats = await fs.stat(file);
  const size = stats.size;

  const logicalFileQuery = `
    ${env.PREFIXES}
    WITH <${env.FILES_GRAPH}>
    INSERT DATA {
      ${sparqlEscapeUri(exportUri)}
        a ${sparqlEscapeUri(env.EXPORT_TYPE)} ,
          nfo:FileDataObject ;
        export:classification ${sparqlEscapeUri(env.EXPORT_CLASSIFICATION_URI)} ;
        mu:uuid ${sparqlEscapeString(exportId)} ;
        nfo:fileName ${sparqlEscapeString(filename)} ;
        dct:format ${sparqlEscapeString(format)} ;
        nfo:fileSize ${sparqlEscapeInt(size)} ;
        dbpedia:fileExtension ${sparqlEscapeString(extension)} ;
        dct:created ${sparqlEscapeDateTime(created)} .
    }`;

  await updateSudo(logicalFileQuery);

  const phyId = uuid();
  const phyUri = file.replace('/share', 'share://');

  const phyFileQuery = `
    ${env.PREFIXES}
    INSERT DATA {
      GRAPH ${sparqlEscapeUri(env.FILES_GRAPH)} {
        ${sparqlEscapeUri(phyUri)}
          a nfo:FileDataObject ;
          a nfo:LocalFileDataObject ;
          nfo:fileName ${sparqlEscapeString(filename)} ;
          nie:dataSource ${sparqlEscapeUri(exportUri)} ;
          mu:uuid ${sparqlEscapeString(phyId)} ;
          dct:format ${sparqlEscapeString(format)} ;
          nfo:fileSize ${sparqlEscapeInt(size)} ;
          dbpedia:fileExtension ${sparqlEscapeString(extension)} ;
          dct:created ${sparqlEscapeDateTime(created)} .
      }
    }`;

  await updateSudo(phyFileQuery);

  return new ExportFile({
    uri: exportUri,
    id: exportId,
    format,
    size,
    created,
  });
}

export default ExportFile;
export { insertNewExportFile };
