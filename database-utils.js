import { querySudo } from '@lblod/mu-auth-sudo';
import * as env from './env';

const isDatabaseUp = async function () {
  let isUp = false;
  try {
    await sendDummyQuery();
    isUp = true;
  } catch (e) {
    console.log(`Waiting for database... ${e}`);
  }
  return isUp;
};

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

const waitForDatabase = async function () {
  let loop = true;
  while (loop) {
    loop = !(await isDatabaseUp());
    await sleep(env.PING_DB_INTERVAL);
  }
};

const sendDummyQuery = async function () {
  try {
    await querySudo(`
      SELECT ?s WHERE {
        GRAPH ?g {
          ?s ?p ?o
        }
      } LIMIT 1
    `);
  } catch (e) {
    throw new Error(e.toString());
  }
};

export { waitForDatabase };
