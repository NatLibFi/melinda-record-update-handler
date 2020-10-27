/* eslint-disable no-unused-vars */
import {promisify} from 'util';
import {createLogger} from '@natlibfi/melinda-backend-commons';
import {eratuontiFactory, COMMON_JOB_STATES, HARVESTER_JOB_STATES} from '@natlibfi/melinda-record-link-migration-commons';
import {BLOB_STATE} from '@natlibfi/melinda-record-import-commons';
import moment from 'moment';

export async function readBlobsFromEratuonti(job, mongoOperator, {apiUrl, apiUsername, apiPassword, apiClientUserAgent}) {
  const logger = createLogger();
  const setTimeoutPromise = promisify(setTimeout);
  const {jobId, jobConfig, blobIds} = job;
  const {linkDataHarvesterApiProfileId} = jobConfig;
  const eratuontiOperator = eratuontiFactory({apiUrl, apiUsername, apiPassword, apiClientUserAgent, linkDataHarvesterApiProfileId}); // eslint-disable-line no-unused-vars
  logger.log('info', `Reading eratuonti blobs from job: ${jobId}`);
  logger.log('info', `Blobs: ${blobIds}`);

  await readBlobs(blobIds);
  // Await mongoOperator.setState({jobId, state: COMMON_JOB_STATES.DONE});

  return true;

  async function readBlobs(blobIds) {
    const [id, ...rest] = blobIds;
    if (id === undefined) {
      return;
    }

    logger.log('debug', `Blob id: ${id}`);

    const result = await eratuontiOperator.readBlob(id);

    /*
    {
      "processingInfo":{
        "numberOfRecords":0,
        "failedRecords":[],
        "importResults":[]
      },
      "state":"PENDING_TRANSFORMATION",
      "id":"87afa7eb-2bee-4c64-980c-9823316fb25f",
      "profile":"foo",
      "contentType":"application/json",
      "creationTime":"2020-10-08T08:47:13+03:00",
      "modificationTime":"2020-10-08T08:47:13+03:00"
    }

    {
      "processingInfo": {
        "numberOfRecords": 1,
        "failedRecords": [],
        "importResults": []
      },
      "state": "TRANSFORMED",
      "id": "5f8f1f26-4701-4ea4-b5c6-5c50b45129fb",
      "profile": "foo",
      "contentType": "application/json",
      "creationTime": "2020-10-14T09:33:24+00:00",
      "modificationTime": "2020-10-14T09:33:33+00:00"
    }

    {
      "processingInfo": {
        "numberOfRecords": 1,
        "failedRecords": [],
        "importResults": [
          {
            "timestamp": "2020-10-14T09:33:42.490Z",
            "status": "SKIPPED",
            "metadata": {
              "title": "Dummy record",
              "standardIdentifiers": "1-dummy-record"
            }
          }
        ]
      },
      "state": "PROCESSED",
      "id": "5f8f1f26-4701-4ea4-b5c6-5c50b45129fb",
      "profile": "foo",
      "contentType": "application/json",
      "creationTime": "2020-10-14T09:33:24+00:00",
      "modificationTime": "2020-10-14T09:33:42+00:00"
    }

    */
    // logger.log('debug', JSON.stringify(result.state, undefined, 2));
    // console.log(result); // eslint-disable-line no-console

    /*Blob states: PENDING_TRANSFORMATION, TRANSFORMATION_IN_PROGRESS, TRANSFORMATION_FAILED, TRANSFORMED, PROCESSED, ABORTED */
    if (result.state === BLOB_STATE.PENDING_TRANSFORMATION) {
      logger.log('info', 'Blob is pending transformation!');
      logger.log('debug', JSON.stringify(result, undefined, 2));
      await setTimeoutPromise(3000);
      return readBlobs([id, ...rest]);
    }

    if (result.state === BLOB_STATE.TRANSFORMATION_IN_PROGRESS) {
      logger.log('info', 'Blob is in transformation!');
      logger.log('debug', JSON.stringify(result, undefined, 2));
      await setTimeoutPromise(3000);
      return readBlobs([id, ...rest]);
    }

    if (result.state === BLOB_STATE.TRANSFORMED) {
      return handleTransformed(result);
    }

    if (result.state === BLOB_STATE.PROCESSED) {
      await logger.log('info', 'Blob prosessed!');
      logger.log('debug', JSON.stringify(result, undefined, 2));
      // FOR TESTING!
      // await resetJobConfigOffset(jobId, jobConfig);
      // await mongoOperator.setState({jobId, state: HARVESTER_JOB_STATES.PENDING_SRU_HARVESTER});
      await mongoOperator.setState({jobId, state: COMMON_JOB_STATES.DONE});
      return readBlobs(rest);
    }

    if (result.state === BLOB_STATE.TRANSFORMATION_FAILED) {
      return handleTransformationFailed(result);
    }

    if (result.state === BLOB_STATE.ABORTED) {
      logger.log('info', 'Blob ABORTED!');
      await mongoOperator.setState({jobId, state: COMMON_JOB_STATES.ABORTED});
      return readBlobs(rest);
    }

    return readBlobs(rest);

    async function handleTransformed(result) {
      logger.log('info', 'Blob is pending to be processed!');
      logger.log('debug', JSON.stringify(result, undefined, 2));
      const timePast = moment(moment(result.modificationTime).add(2, 'm').format()).isSameOrBefore(moment().format());
      const hasRecords = result.processingInfo.numberOfRecords > 0;
      logger.log('info', `Last modification time has been 2 mins ago: ${timePast}`);
      if (!timePast) { // eslint-disable-line functional/no-conditional-statement
        logger.log('debug', `Current time: ${moment().format()}`);
        logger.log('debug', `Last modification time: ${moment(result.modificationTime).format()}`);
      }
      logger.log('info', `Blob has at least one transformed record: ${hasRecords}`);
      if (timePast && hasRecords) {
        logger.log('info', 'Mark as processed!');
        await eratuontiOperator.updateBlobState(id, BLOB_STATE.PROCESSED);
        await setTimeoutPromise(10000);
        return readBlobs([id, ...rest]);
      }
      await setTimeoutPromise(15000);
      return readBlobs([id, ...rest]);
    }

    async function handleTransformationFailed(result) {
      logger.log('info', 'Blob transformation failed!');
      logger.log('info', JSON.stringify(job, undefined, 2));
      logger.log('info', JSON.stringify(result, undefined, 2));
      // IF result.processingInfo.transformationError is X restart else set state COMMON_JOB_STATES.ERROR

      if (jobConfig.linkDataHarvestSearch.type === 'sru') {
        await resetJobConfigOffset(jobId, jobConfig);
        await mongoOperator.setState({jobId, state: HARVESTER_JOB_STATES.PENDING_SRU_HARVESTER});
        return readBlobs(rest);
      }

      if (jobConfig.linkDataHarvestSearch.type === 'oai-pmh') {
        await resetJobConfigResumptionToken(jobId, jobConfig);
        await mongoOperator.setState({jobId, state: HARVESTER_JOB_STATES.PENDING_OAI_PMH_HARVESTER});
        return readBlobs(rest);
      }

      if (jobConfig.linkDataHarvestSearch.type === 'finto') {
        await mongoOperator.setState({jobId, state: HARVESTER_JOB_STATES.PENDING_OAI_PMH_HARVESTER});
        return readBlobs(rest);
      }

      await mongoOperator.setState({jobId, state: COMMON_JOB_STATES.ERROR});
      return readBlobs(rest);
    }

    async function resetJobConfigOffset(jobId, jobConfig) {
      const updatedJobConfig = {
        sourceRecord: jobConfig.sourceRecord,
        linkDataHarvestSearch: {
          type: jobConfig.linkDataHarvestSearch.type,
          from: jobConfig.linkDataHarvestSearch.from,
          queryFormat: jobConfig.linkDataHarvestSearch.queryFormat,
          url: jobConfig.linkDataHarvestSearch.url,
          offset: 0
        },
        linkDataHarvesterApiProfileId: jobConfig.linkDataHarvesterApiProfileId,
        linkDataHarvesterValidationFilters: jobConfig.linkDataHarvesterValidationFilters
      };
      await mongoOperator.updateJobConfig({jobId, jobConfig: updatedJobConfig});
    }

    async function resetJobConfigResumptionToken(jobId, jobConfig) {
      const updatedJobConfig = {
        sourceRecord: jobConfig.sourceRecord,
        linkDataHarvestSearch: {
          type: jobConfig.linkDataHarvestSearch.type,
          from: jobConfig.linkDataHarvestSearch.from,
          url: jobConfig.linkDataHarvestSearch.url,
          resumptionToken: ''
        },
        linkDataHarvesterApiProfileId: jobConfig.linkDataHarvesterApiProfileId,
        linkDataHarvesterValidationFilters: jobConfig.linkDataHarvesterValidationFilters
      };
      await mongoOperator.updateJobConfig({jobId, jobConfig: updatedJobConfig});
    }
  }
}
