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
use Symfony\Component\Stopwatch\Stopwatch;
use Keboola\Syrup\Exception\UserException;

class LoadDataMulti extends AbstractTask
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
     * required: pid, tables
     */
    public function run(Job $job, $taskId, array $params = [], $definitionFile = null)
    {
        $this->initRestApi($job);
        $this->checkParams($params, ['pid', 'tables']);
        $project = $this->configuration->getProject($params['pid']);
        if (!$project) {
            throw new UserException($this->translator->trans('parameters.pid_not_configured'));
        }
        if (!$definitionFile) {
            throw new UserException($this->translator->trans('job_executor.data_set_definition_missing'));
        }

        $bucketAttributes = $this->configuration->bucketAttributes();

        $stopWatch = new Stopwatch();

        $definition = $this->getDefinition($definitionFile);

        // Init
        $tmpFolderName = basename($this->getTmpDir($job->getId())) . '/' . $taskId;
        $csvHandler = new CsvHandler($this->temp, $this->scriptsPath, $this->storageApiClient, $this->logger);
        $csvHandler->setJobId($job->getId());
        $csvHandler->setRunId($job->getRunId());

        $this->restApi->login($bucketAttributes['gd']['username'], $bucketAttributes['gd']['password']);


        // Get manifest
        $manifest = [];
        foreach ($definition as $tableId => &$def) {
            $def['incrementalLoad'] = !empty($def['dataset']['incrementalLoad']) ? $def['dataset']['incrementalLoad'] : 0;
            $def['filterColumn'] = $this->getFilterColumn($tableId, $def['dataset'], $bucketAttributes);
            $def['filterColumn'] = ($def['filterColumn'] && empty($project['main'])) ? $def['filterColumn'] : false;
            $manifest[] = Model::getDataLoadManifest($tableId, $def['columns'], $def['incrementalLoad'], $this->configuration->noDateFacts);
        }
        $manifest = ['dataSetSLIManifestList' => $manifest];
        file_put_contents($this->getTmpDir($job->getId()) . '/upload_info.json', json_encode($manifest));

        try {
            // Upload to WebDav
            $webDav = new WebDav($bucketAttributes['gd']['username'], $bucketAttributes['gd']['password']);
            $webDav->prepareFolder($tmpFolderName);

            // Upload dataSets
            foreach ($definition as $tableId => $d) {
                $stopWatchId = 'transfer_csv_' . $tableId;
                $stopWatch->start($stopWatchId);

                $webDavFileUrl = sprintf('%s/%s/%s.csv', $webDav->getUrl(), $tmpFolderName, $tableId);
                try {
                    $csvHandler->runUpload(
                        $bucketAttributes['gd']['username'],
                        $bucketAttributes['gd']['password'],
                        $webDavFileUrl,
                        $d['columns'],
                        $tableId,
                        $d['incrementalLoad'],
                        $d['filterColumn'],
                        $params['pid'],
                        $this->configuration->noDateFacts
                    );
                } catch (UserException $e) {
                    throw new UserException(sprintf("Error during upload of dataset '%s': %s", $tableId, $e->getMessage()), $e);
                }
                if (!$webDav->fileExists(sprintf('%s/%s.csv', $tmpFolderName, $tableId))) {
                    throw new UserException($this->translator->trans('error.csv_not_uploaded %1', ['%1' => $webDavFileUrl]));
                }

                $this->logEvent(
                    'Csv for '.$tableId.' transferred to GoodData',
                    $taskId,
                    $job->getId(),
                    $job->getRunId(),
                    ['url' => $webDavFileUrl],
                    $stopWatch->stop($stopWatchId)->getDuration()
                );
            }


            $stopWatchId = 'upload_manifest';
            $stopWatch->start($stopWatchId);

            $webDav->upload($this->getTmpDir($job->getId()) . '/upload_info.json', $tmpFolderName);
            $this->logEvent(
                'Manifest transferred to GoodData',
                $taskId,
                $job->getId(),
                $job->getRunId(),
                ['url' => $webDav->getUrl() . '/' . $tmpFolderName . '/upload_info.json'],
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

        return [];
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
