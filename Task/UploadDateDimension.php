<?php
/**
 * Created by PhpStorm.
 * User: JakubM
 * Date: 10.02.14
 * Time: 10:54
 */

namespace Keboola\GoodDataWriter\Task;

use Keboola\GoodDataWriter\Exception\RestApiException;
use Keboola\GoodDataWriter\GoodData\WebDav;
use Keboola\GoodDataWriter\GoodData\Model;
use Keboola\GoodDataWriter\Job\Metadata\Job;
use Keboola\StorageApi\Event;
use Keboola\Syrup\Exception\UserException;
use Symfony\Component\Stopwatch\Stopwatch;

class UploadDateDimension extends AbstractTask
{
    public $eventsLog;

    public function prepare($params)
    {
        $this->checkParams($params, ['writerId', 'name']);
        $this->checkWriterExistence($params['writerId']);

        $dateDimensions = $this->configuration->getDateDimensions();
        if (!in_array($params['name'], array_keys($dateDimensions))) {
            throw new UserException($this->translator->trans('parameters.dimension_name'));
        }

        return [
            'name' => $params['name'],
            'includeTime' => $dateDimensions[$params['name']]['includeTime']
        ];
    }

    /**
     * required: pid, name
     * optional:
     */
    public function run(Job $job, $taskId, array $params = [], $definitionFile = null)
    {
        $this->initRestApi($job);
        $this->checkParams($params, ['pid', 'name']);
        $project = $this->configuration->getProject($params['pid']);
        if (!$project) {
            throw new UserException($this->translator->trans('parameters.pid_not_configured'));
        }

        $stopWatch = new Stopwatch();

        $bucketAttributes = $this->configuration->getBucketAttributes();

        $dateDimensions = $this->configuration->getDateDimensions();
        if (!in_array($params['name'], array_keys($dateDimensions))) {
            throw new UserException($this->translator->trans('parameters.dimension_name'));
        }

        // Init
        $tmpFolderName = basename($this->getTmpDir($job->getId()));
        $this->restApi->login($bucketAttributes['gd']['username'], $bucketAttributes['gd']['password']);

        $includeTime = $dateDimensions[$params['name']]['includeTime'];
        $template = $dateDimensions[$params['name']]['template'];

        $stopWatchId = 'createDimension-' . $params['name'];
        $stopWatch->start($stopWatchId);

        try {
            // Create date dimensions
            $this->restApi->createDateDimension($params['pid'], $params['name'], $includeTime, $template);

            $e = $stopWatch->stop($stopWatchId);
            $this->logEvent('Date dimension created', $taskId, $job->getId(), $job->getRunId(), [], $e->getDuration());

            if ($includeTime) {
                // Upload to WebDav
                $webDav = new WebDav($bucketAttributes['gd']['username'], $bucketAttributes['gd']['password']);

                // Upload time dimension data
                $stopWatchId = 'uploadTimeDimension-' . $params['name'];
                $stopWatch->start($stopWatchId);

                $dimensionName = Model::getId($params['name']);
                $tmpFolderDimension = $this->getTmpDir($job->getId()) . '/' . $dimensionName;
                $tmpFolderNameDimension = $tmpFolderName . '-' . $dimensionName;

                mkdir($tmpFolderDimension);
                $timeDimensionManifest = Model::getTimeDimensionDataLoadManifest($this->scriptsPath, $params['name']);
                file_put_contents($tmpFolderDimension . '/upload_info.json', $timeDimensionManifest);
                copy($this->scriptsPath . '/time-dimension.csv', $tmpFolderDimension . '/' . $dimensionName . '.csv');
                $webDav->prepareFolder($tmpFolderNameDimension);
                $webDav->upload($tmpFolderDimension . '/upload_info.json', $tmpFolderNameDimension);
                $webDav->upload($tmpFolderDimension . '/' . $dimensionName . '.csv', $tmpFolderNameDimension);
                $dimensionsToUpload[] = $params['name'];

                $e = $stopWatch->stop($stopWatchId);
                $this->logEvent('Time dimension data uploaded to GoodData', $taskId, $job->getId(), $job->getRunId(), [
                    'destination' => $webDav->getUrl() . '/uploads/' . $tmpFolderNameDimension
                ], $e->getDuration());

                // Run ETL task of time dimensions
                $stopWatchId = sprintf('runEtlTimeDimension-%s', $params['name']);
                $stopWatch->start($stopWatchId);

                $dataSetName = 'time.' . $dimensionName;
                try {
                    $this->restApi->loadData($params['pid'], $tmpFolderNameDimension);
                } catch (RestApiException $e) {
                    $debugFile = $tmpFolderDimension . '/' . $params['pid'] . '-etl.log';
                    $logSaved = $webDav->saveLogs($tmpFolderDimension, $debugFile);
                    if ($logSaved) {
                        if (filesize($debugFile) > 1024 * 1024) {
                            $e->setData($this->s3Client->uploadFile(
                                $debugFile,
                                'text/plain',
                                sprintf('%s/%s/%s-etl.log', $tmpFolderName, $params['pid'], $dataSetName),
                                true
                            ));
                        } else {
                            $e->setData(file_get_contents($debugFile));
                        }
                    }

                    throw $e;
                }

                $e = $stopWatch->stop($stopWatchId);
                $this->logEvent(
                    'Time dimension data processing finished',
                    $taskId,
                    $job->getId(),
                    $job->getRunId(),
                    [],
                    $e->getDuration()
                );
            }

        } catch (\Exception $e) {
            $params = ['error' => $e->getMessage()];
            if ($e instanceof UserException) {
                $params['details'] = $e->getData();
            }
            $this->logEvent(
                'Time dimension data processing failed',
                $taskId,
                $job->getId(),
                $job->getRunId(),
                $params,
                $stopWatch->stop($stopWatchId)->getDuration(),
                Event::TYPE_ERROR
            );

            throw $e;
        }

        $this->configuration->setDateDimensionIsExported($params['name'], true);
        return [];
    }
}
