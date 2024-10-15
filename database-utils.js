import { querySudo } from '@lblod/mu-auth-sudo';
import * as env from './env';

async function isDatabaseUp() {
  let isUp = false;
  try {
    await sendDummyQuery();
    isUp = true;
  } catch (e) {
    console.log(`Waiting for database... ${e}`);
  }
  return isUp;
}

export function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

export async function waitForDatabase() {
  let loop = true;
  while (loop) {
    loop = !(await isDatabaseUp());
    await sleep(env.PING_DB_INTERVAL);
  }
}

async function sendDummyQuery() {
  await querySudo(`
    SELECT ?s WHERE {
      GRAPH ?g {
        ?s ?p ?o
      }
    } LIMIT 1`);
}
