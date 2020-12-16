/* eslint-disable no-unused-vars, no-undef, no-warning-comments */
import {promisify} from 'util';
import {createLogger} from '@natlibfi/melinda-backend-commons';
import {eratuontiFactory, mongoFactory, COMMON_JOB_STATES} from '@natlibfi/melinda-record-link-migration-commons';
import {readBlobsFromEratuonti} from './interfaces/eratuonti';

export default async function ({apiUrl, apiUsername, apiPassword, apiClientUserAgent, mongoUrl}) {
  const logger = createLogger();
  const mongoOperator = await mongoFactory(mongoUrl);
  const eratuontiOperator = eratuontiFactory({apiUrl, apiUsername, apiPassword, apiClientUserAgent}); // eslint-disable-line no-unused-vars
  const setTimeoutPromise = promisify(setTimeout);
  logger.log('info', 'Melinda-eratuonti-watcher-link-migration has started');

  return check();

  async function check(wait) {
    await checkJobsInState(COMMON_JOB_STATES.PENDING_ERATUONTI);

    throw new Error('Checks done - Shutting down!');
  }

  async function checkJobsInState(state) {
    const promises = [];
    const cursor = await mongoOperator.getAll(state);
    await cursor.forEach(handlejob);
    await Promise.all(promises);

    async function handlejob(job) {
      logger.log('debug', `Job id: ${job.jobId}`);
      // logger.log('info', JSON.stringify(job, undefined, 2));
      // logger.log('debug', JSON.stringify(job.blobIds));

      if (job.blobIds.length < 1) { // eslint-disable-line functional/no-conditional-statement
        return promises.push(await mongoOperator.setState({jobId: job.jobId, state: COMMON_JOB_STATES.ERROR})); // eslint-disable-line functional/immutable-data
      }

      return promises.push(readBlobsFromEratuonti(job, mongoOperator, eratuontiOperator)); // eslint-disable-line functional/immutable-data
    }
  }
}
