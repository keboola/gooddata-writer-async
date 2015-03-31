<?php
/**
 * @author Jakub Matejka <jakub@keboola.com>
 * @date 2013-04-17
 */

namespace Keboola\GoodDataWriter\Task;

use Keboola\GoodDataWriter\Exception\RestApiException;
use Keboola\GoodDataWriter\GoodData\CsvHandler;
use Keboola\GoodDataWriter\GoodData\WebDav;
use Keboola\GoodDataWriter\GoodData\Model;
use Keboola\GoodDataWriter\Job\Metadata\Job;
use Keboola\StorageApi\Event;
use Keboola\Syrup\Exception\UserException;
use Symfony\Component\Stopwatch\Stopwatch;

class LoadData extends AbstractTask
{

    public function prepare($params)
    {
        $this->checkParams($params, ['writerId']);

        if (isset($params['tables'])) {
            if (!is_array($params['tables'])) {
                throw new UserException($this->translator->trans('parameters.tables_not_array'));
            }
        } else {
            $params['tables'] = [];
            foreach ($this->configuration->getDataSets() as $dataSet) {
                if (!empty($dataSet['export'])) {
                    $params['tables'][] = $dataSet['id'];
                }
            }
        }
        $this->checkWriterExistence($params['writerId']);
        $result = [
            'tables' => $params['tables']
        ];
        if (isset($params['incrementalLoad'])) {
            $result['incrementalLoad'] = $params['incrementalLoad'];
        }
        return $result;
    }

    /**
     * required: pid, tableId
     * optional: incrementalLoad
     */
    public function run(Job $job, $taskId, array $params = [], $definitionFile = null)
    {
        $this->initRestApi($job);
        $this->checkParams($params, ['pid', 'tableId']);
        $project = $this->configuration->getProject($params['pid']);
        if (!$project) {
            throw new UserException($this->translator->trans('parameters.pid_not_configured'));
        }
        if (empty($definitionFile)) {
            throw new UserException($this->translator->trans('job_executor.data_set_definition_missing'));
        }

        $bucketAttributes = $this->configuration->getBucketAttributes();
        $this->configuration->updateDataSetsFromSapi();

        $stopWatch = new Stopwatch();

        // Init
        $tmpFolderName = basename($this->getTmpDir($job->getId())) . '/' . $taskId;
        $csvHandler = new CsvHandler($this->temp, $this->scriptsPath, $this->storageApiClient, $this->logger);
        $csvHandler->setJobId($job->getId());
        $csvHandler->setRunId($job->getRunId());

        $tableDefinition = $this->configuration->getDataSet($params['tableId']);
        $incrementalLoad = (isset($params['incrementalLoad'])) ? $params['incrementalLoad']
            : (!empty($tableDefinition['incrementalLoad']) ? $tableDefinition['incrementalLoad'] : 0);
        $filterColumn = $this->getFilterColumn($params['tableId'], $tableDefinition, $bucketAttributes);
        $filterColumn = ($filterColumn && empty($project['main'])) ? $filterColumn : false;

        $this->restApi->login($bucketAttributes['gd']['username'], $bucketAttributes['gd']['password']);


        // Get definition
        $stopWatchId = 'get_definition';
        $stopWatch->start($stopWatchId);

        $definition = $this->getDefinition($definitionFile);

        $this->logEvent(
            'Dataset definition fetched',
            $taskId,
            $job->getId(),
            $job->getRunId(),
            ['definition' => $definitionFile],
            $stopWatch->stop($stopWatchId)->getDuration()
        );

        // Get manifest
        $manifest = Model::getDataLoadManifest($params['tableId'], $definition, $incrementalLoad, $this->configuration->noDateFacts);
        file_put_contents($this->getTmpDir($job->getId()) . '/upload_info.json', json_encode($manifest));

        try {
            // Upload to WebDav
            $webDav = new WebDav($bucketAttributes['gd']['username'], $bucketAttributes['gd']['password']);

            // Upload dataSets
            $stopWatchId = 'transfer_csv';
            $stopWatch->start($stopWatchId);

            $webDav->prepareFolder($tmpFolderName);

            $webDavFileUrl = sprintf('%s/%s/%s.csv', $webDav->getUrl(), $tmpFolderName, $params['tableId']);
            $csvHandler->runUpload(
                $bucketAttributes['gd']['username'],
                $bucketAttributes['gd']['password'],
                $webDavFileUrl,
                $definition,
                $params['tableId'],
                $incrementalLoad,
                $filterColumn,
                $params['pid'],
                $this->configuration->noDateFacts
            );
            if (!$webDav->fileExists(sprintf('%s/%s.csv', $tmpFolderName, $params['tableId']))) {
                throw new UserException($this->translator->trans(
                    'error.csv_not_uploaded %1',
                    ['%1' => $webDavFileUrl]
                ));
            }

            $webDav->upload($this->getTmpDir($job->getId()) . '/upload_info.json', $tmpFolderName);

            $this->logEvent(
                'Csv transferred to GoodData',
                $taskId,
                $job->getId(),
                $job->getRunId(),
                ['data' => $webDavFileUrl, 'manifest' => $webDav->getUrl() . '/' . $tmpFolderName . '/upload_info.json'],
                $stopWatch->stop($stopWatchId)->getDuration()
            );


            // Run ETL task of dataSets
            $stopWatchId = 'run_etl';
            $stopWatch->start($stopWatchId);

            try {
                $this->restApi->loadData($params['pid'], $tmpFolderName);
            } catch (RestApiException $e) {
                $debugFile = $this->getTmpDir($job->getId()) . '/etl.log';
                $logSaved = $webDav->saveLogs($tmpFolderName, $debugFile);
                if ($logSaved) {
                    if (filesize($debugFile) > 1024 * 1024) {
                        $e->setData([$this->s3Client->uploadFile(
                            $debugFile,
                            'text/plain',
                            sprintf('%s/etl.log', $tmpFolderName),
                            true
                        )]);
                    } else {
                        $e->setData(file_get_contents($debugFile));
                    }
                }

                throw $e;
            }
            $this->logEvent(
                'Csv processing in GoodData finished',
                $taskId,
                $job->getId(),
                $job->getRunId(),
                [],
                $stopWatch->stop($stopWatchId)->getDuration()
            );
        } catch (\Exception $e) {
            $this->logEvent(
                'Csv processing in GoodData failed',
                $taskId,
                $job->getId(),
                $job->getRunId(),
                ['error' => $e->getMessage()],
                $stopWatch->isStarted($stopWatchId)? $stopWatch->stop($stopWatchId)->getDuration() : 0,
                Event::TYPE_ERROR
            );

            throw $e;
        }

        $result = [];
        if ($incrementalLoad) {
            $result['flags'] = ['incremental' => $this->translator->trans('result.flag.incremental %1', ['%1' => $incrementalLoad])];
        }
        return $result;
    }


    private function getFilterColumn($tableId, $tableDefinition, $bucketAttributes)
    {
        $filterColumn = null;
        if (isset($bucketAttributes['filterColumn']) && empty($tableDefinition['ignoreFilter'])) {
            $filterColumn = $bucketAttributes['filterColumn'];
            $tableInfo = $this->configuration->getSapiTable($tableId);
            if (!in_array($filterColumn, $tableInfo['columns'])) {
                throw new UserException($this->translator->trans('configuration.upload.filter_missing', ['%1' => $filterColumn]));
            }
            if (!in_array($filterColumn, $tableInfo['indexedColumns'])) {
                throw new UserException($this->translator->trans('configuration.upload.filter_index_missing', ['%1' => $filterColumn]));
            }
        }
        return $filterColumn;
    }
}
