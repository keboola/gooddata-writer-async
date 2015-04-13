<?php
/**
 * @author Jakub Matejka <jakub@keboola.com>
 * @date 2013-04-17
 */

namespace Keboola\GoodDataWriter\Task;

use Keboola\GoodDataWriter\Job\Metadata\Job;
use Keboola\StorageApi\Event;
use Keboola\Syrup\Exception\UserException;
use Symfony\Component\Stopwatch\Stopwatch;

class UpdateModel extends AbstractTask
{

    public function prepare($params)
    {
        $this->checkParams($params, ['writerId', 'tableId']);
        $this->checkWriterExistence($params['writerId']);

        return [
            'tableId' => $params['tableId']
        ];
    }

    /**
     * required: pid, tableId
     * optional:
     */
    public function run(Job $job, $taskId, array $params = [], $definitionFile = null)
    {
        $this->initRestApi($job);
        $this->checkParams($params, ['pid', 'tableId']);
        $project = $this->configuration->getProject($params['pid']);
        if (!$project) {
            throw new UserException($this->translator->trans('parameters.pid_not_configured'));
        }
        if (!$definitionFile) {
            throw new UserException($this->translator->trans('job_executor.data_set_definition_missing'));
        }

        $bucketAttributes = $this->configuration->getBucketAttributes();
        $this->configuration->updateDataSetsFromSapi();

        $stopWatch = new Stopwatch();


        // Init
        $this->restApi->login($bucketAttributes['gd']['username'], $bucketAttributes['gd']['password']);

        $tableDefinition = $this->configuration->getDataSet($params['tableId']);

        // Get definition
        $stopWatchId = 'get_definition';
        $stopWatch->start($stopWatchId);

        $definition = $this->getDefinition($definitionFile);

        $this->logEvent(
            'Definition downloaded',
            $taskId,
            $job->getId(),
            $job->getRunId(),
            ['file' => $definitionFile],
            $stopWatch->stop($stopWatchId)->getDuration()
        );

        $updateOperations = [];
        $ldmChange = false;
        try {
            // Update model
            $stopWatchId = 'GoodData';
            $stopWatch->start($stopWatchId);

            $updateResult = $this->restApi->updateDataSet($params['pid'], $definition, $this->configuration->noDateFacts);
            if ($updateResult) {
                $updateOperations[] = $updateResult['description'];
                $ldmChange = true;
            }

            if (empty($tableDefinition['isExported'])) {
                // Save export status to definition
                $this->configuration->updateDataSetDefinition($params['tableId'], 'isExported', 1);
            }

            if ($ldmChange) {
                $this->logEvent(
                    'Model updated',
                    $taskId,
                    $job->getId(),
                    $job->getRunId(),
                    ['maql' => $updateResult['maql']],
                    $stopWatch->stop($stopWatchId)->getDuration()
                );
            } else {
                $this->logEvent(
                    'Model has not been changed',
                    $taskId,
                    $job->getId(),
                    $job->getRunId()
                );
            }

        } catch (\Exception $e) {
            $params = ['error' => $e->getMessage()];
            if ($e instanceof UserException) {
                $params['details'] = $e->getData();
            }
            $this->logEvent(
                'Model update failed',
                $taskId,
                $job->getId(),
                $job->getRunId(),
                $params,
                $stopWatch->stop($stopWatchId)->getDuration(),
                Event::TYPE_ERROR
            );

            throw $e;
        }

        $result = [];
        if (count($updateOperations)) {
            $result['info'] = $updateOperations;
        }
        if ($ldmChange) {
            $result['flags'] = ['ldm' => $this->translator->trans('result.flag.ldm')];
        }
        return $result;
    }
}
