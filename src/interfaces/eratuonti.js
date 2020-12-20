/* eslint-disable no-unused-vars */
import {promisify} from 'util';
import {createLogger} from '@natlibfi/melinda-backend-commons';
import {COMMON_JOB_STATES, HARVESTER_JOB_STATES} from '@natlibfi/melinda-record-update-commons';
import {BLOB_STATE} from '@natlibfi/melinda-record-import-commons';

export async function readBlobsFromEratuonti(job, mongoOperator, eratuontiOperator) {
  const logger = createLogger();
  const {jobId, jobConfig, blobIds} = job;
  logger.log('info', `Reading eratuonti blobs from job: ${jobId}`);
  logger.log('info', `Blobs: ${blobIds}`);

  const {needAction, done} = await readBlobs(blobIds);

  if (needAction) {
    // All blobs handled as far as they could be
    if (done === blobIds.length) {
      await handleReset();
      return;
    }
    return;
  }

  // All fine and dandy
  if (done === blobIds.length) {
    await mongoOperator.setState({jobId, state: COMMON_JOB_STATES.DONE});
    return;
  }

  async function readBlobs(blobIds = [], needRestart = false, done = 0) {
    const [id, ...rest] = blobIds;
    if (id === undefined) {
      return {needAction: needRestart, done};
    }

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
    // logger.log('debug', `Blob id: ${id}`);
    if (result.state === BLOB_STATE.PENDING_TRANSFORMATION) {
      logger.log('silly', 'Blob is pending transformation!');
      logger.log('silly', JSON.stringify(result, undefined, 2));
      return readBlobs(rest, needRestart, true, done);
    }

    if (result.state === BLOB_STATE.TRANSFORMATION_IN_PROGRESS) {
      logger.log('silly', 'Blob is in transformation!');
      logger.log('silly', JSON.stringify(result, undefined, 2));
      return readBlobs(rest, needRestart, true, done);
    }

    if (result.state === BLOB_STATE.TRANSFORMED) {
      logger.log('silly', 'Blob has been transformed!');
      logger.log('silly', JSON.stringify(result, undefined, 2));
      return readBlobs(rest, needRestart, true, done);
    }

    if (result.state === BLOB_STATE.PROCESSING) {
      logger.log('silly', 'Blob is in processing!');
      logger.log('silly', JSON.stringify(result, undefined, 2));
      return readBlobs(rest, needRestart, true, done);
    }

    if (result.state === BLOB_STATE.PROCESSED) {
      logger.log('debug', `Blob id: ${id}`);
      logger.log('info', 'Blob prosessed!');
      const notDone = handleProsessed(result);

      if (!notDone) {
        logger.log('silly', JSON.stringify(result, undefined, 2));
        return readBlobs(rest, needRestart, done);
      }
      const newDone = done + 1;
      return readBlobs(rest, !notDone, newDone);
    }

    if (result.state === BLOB_STATE.TRANSFORMATION_FAILED) {
      logger.log('info', 'Blob transformation failed!');
      logger.log('debug', JSON.stringify(result, undefined, 2));
      const newDone = done + 1;
      return readBlobs(rest, true, newDone);
    }

    if (result.state === BLOB_STATE.ABORTED) {
      logger.log('debug', `Blob id: ${id}`);
      logger.log('info', 'Blob ABORTED!');
      const newDone = done + 1;
      return readBlobs(rest, true, newDone);
    }

    return readBlobs(rest, needRestart, done);

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
      await mongoOperator.setState({jobId, state: HARVESTER_JOB_STATES.PENDING_FINTO_HARVESTER});
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
