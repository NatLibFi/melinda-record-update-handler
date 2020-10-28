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

  const needAction = await readBlobs(blobIds);
  if (needAction) {
    await handleReset();
    return;
  }

  await mongoOperator.setState({jobId, state: COMMON_JOB_STATES.DONE});
  return;

  async function readBlobs(blobIds, needRestart = false, wait = false) {
    if (wait) {
      await setTimeoutPromise(3000);
      return readBlobs(blobIds);
    }

    const [id, ...rest] = blobIds;
    if (id === undefined) {
      if (needRestart) {
        return true;
      }

      return false;
    }

    logger.log('debug', `Blob id: ${id}`);

    const result = await eratuontiOperator.readBlob(id);

    /*
    {
      "processingInfo": {
        "numberOfRecords": 1,
        "failedRecords": [],
        "importResults": [
          {
            "timestamp": "2020-10-28T11:26:32.502Z",
            "status": "SKIPPED",
            "metadata": {
              "id": "016122561",
              "reason": "409 - Modification history mismatch (CAT)"
            }
          },
          {
            "timestamp": "2020-10-28T10:03:07.695Z",
            "status": "UPDATED",
            "metadata": {
              "id": "016122573"
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
      logger.log('silly', JSON.stringify(result, undefined, 2));
      return readBlobs([id, ...rest], needRestart, true);
    }

    if (result.state === BLOB_STATE.TRANSFORMATION_IN_PROGRESS) {
      logger.log('info', 'Blob is in transformation!');
      logger.log('silly', JSON.stringify(result, undefined, 2));
      return readBlobs([id, ...rest], needRestart, true);
    }

    if (result.state === BLOB_STATE.TRANSFORMED) {
      logger.log('info', 'Blob has been transformed!');
      logger.log('silly', JSON.stringify(result, undefined, 2));
      return readBlobs([id, ...rest], needRestart, true);
    }

    if (result.state === BLOB_STATE.PROCESSED) {
      await logger.log('info', 'Blob prosessed!');
      const done = handleProsessed(result);

      if (!done) {
        logger.log('debug', JSON.stringify(result, undefined, 2));
        return readBlobs(rest, needRestart || !done);
      }

      return readBlobs(rest, !done);
    }

    if (result.state === BLOB_STATE.TRANSFORMATION_FAILED) {
      logger.log('info', 'Blob transformation failed!');
      logger.log('debug', JSON.stringify(result, undefined, 2));

      return readBlobs(rest, true);
    }

    if (result.state === BLOB_STATE.ABORTED) {
      logger.log('info', 'Blob ABORTED!');
      await mongoOperator.setState({jobId, state: COMMON_JOB_STATES.ABORTED});
      return readBlobs(rest, needRestart);
    }

    return readBlobs(rest, needRestart, true);

    function handleProsessed(result) {
      const {importResults} = result.processingInfo;
      const needRestart = importResults.filter(note => {
        if (note.status === 'SKIPPED' && note.metadata.reason === '409 - Modification history mismatch (CAT)') {
          return true;
        }

        return false;
      });

      return needRestart.length < 1;
    }
  }

  async function handleReset() {
    logger.log('info', JSON.stringify(job, undefined, 2));

    if (jobConfig.linkDataHarvestSearch.type === 'sru') {
      await resetJobConfigOffset(jobId, jobConfig);
      await mongoOperator.setState({jobId, state: HARVESTER_JOB_STATES.PENDING_SRU_HARVESTER});
      return;
    }

    if (jobConfig.linkDataHarvestSearch.type === 'oai-pmh') {
      await resetJobConfigResumptionToken(jobId, jobConfig);
      await mongoOperator.setState({jobId, state: HARVESTER_JOB_STATES.PENDING_OAI_PMH_HARVESTER});
      return;
    }

    if (jobConfig.linkDataHarvestSearch.type === 'finto') {
      await mongoOperator.setState({jobId, state: HARVESTER_JOB_STATES.PENDING_OAI_PMH_HARVESTER});
      return;
    }

    await mongoOperator.setState({jobId, state: COMMON_JOB_STATES.ERROR});
    return;

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
