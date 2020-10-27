/* eslint-disable no-unused-vars, no-undef, no-warning-comments */
import {promisify} from 'util';
import {createLogger} from '@natlibfi/melinda-backend-commons';
import {mongoFactory, COMMON_JOB_STATES} from '@natlibfi/melinda-record-link-migration-commons';
import {readBlobsFromEratuonti} from './interfaces/eratuonti';
export default async function ({
  apiUrl, apiUsername, apiPassword, apiClientUserAgent, mongoUrl, amqpUrl
}) {
  const logger = createLogger();
  const eratuontiConfig = {apiUrl, apiUsername, apiPassword, apiClientUserAgent};
  const mongoOperator = await mongoFactory(mongoUrl);
  const setTimeoutPromise = promisify(setTimeout);
  logger.log('info', 'Melinda-eratuonti-watcher-link-migration has started');

  return check();

  async function check(wait) {
    if (wait) {
      await setTimeoutPromise(3000);
      return check();
    }

    await checkJobsInState(COMMON_JOB_STATES.PENDING_ERATUONTI);

    // Loop
    return check(true);
  }

  async function checkJobsInState(state) {
    const job = await mongoOperator.getOne(state);

    if (job) {
      // logger.log('info', JSON.stringify(job, undefined, 2));
      // logger.log('debug', JSON.stringify(job.blobIds));

      if (job.blobIds.length < 1) { // eslint-disable-line functional/no-conditional-statement
        await mongoOperator.setState({jobId: job.jobId, state: COMMON_JOB_STATES.ERROR});
      }

      await readBlobsFromEratuonti(job, mongoOperator, eratuontiConfig);
    }
  }
}
