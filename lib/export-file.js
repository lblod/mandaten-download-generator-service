import path from 'path';
import fs from 'fs-extra';
import { query, update, uuid,
         sparqlEscapeString, sparqlEscapeUri, sparqlEscapeInt, sparqlEscapeDateTime } from 'mu';

class ExportFile {
  // uri: null;
  // created: null;
  // format: null;
  // size: null;
  constructor(content) {
    for( var key in content )
      this[key] = content[key];
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
  const stats = await fs.stat(file);
  const size = stats.size;
  
  await update(
    `PREFIX export: <http://mu.semte.ch/vocabularies/ext/export/>
     PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
     PREFIX nfo: <http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#>
     PREFIX dct: <http://purl.org/dc/terms/>

     WITH <http://mu.semte.ch/application>
     INSERT DATA { 
       ${sparqlEscapeUri(exportUri)} a export:Export, nfo:FileDataObject ; 
            mu:uuid ${sparqlEscapeString(exportId)} ;
            nfo:filename ${sparqlEscapeString(filename)} ;
            dct:format ${sparqlEscapeString(format)} ;
            nfo:fileSize ${sparqlEscapeInt(size)} ;
            dct:created ${sparqlEscapeDateTime(created)} .
     }`);

  return new ExportFile({
    uri: exportUri,
    id: exportId,
    format,
    size,
    created
  });
}

export default ExportFile;
export { insertNewExportFile };
