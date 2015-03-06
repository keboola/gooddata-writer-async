<?php
/**
 * @author Jakub Matejka <jakub@keboola.com>
 * @date 2013-04-17
 */

namespace Keboola\GoodDataWriter\Task;

use Keboola\GoodDataWriter\Exception\WrongConfigurationException;
use Keboola\GoodDataWriter\Exception\RestApiException;
use Keboola\GoodDataWriter\Exception\WrongParametersException;
use Keboola\GoodDataWriter\Writer\Job;
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
            throw new WrongParametersException($this->translator->trans('parameters.pid_not_configured'));
        }
        if (!$definitionFile) {
            throw new WrongConfigurationException($this->translator->trans('job_executor.data_set_definition_missing'));
        }

        $bucketAttributes = $this->configuration->bucketAttributes();
        $this->configuration->updateDataSetsFromSapi();

        $stopWatch = new Stopwatch();


        // Init
        $this->restApi->login($bucketAttributes['gd']['username'], $bucketAttributes['gd']['password']);

        $tableDefinition = $this->configuration->getDataSet($params['tableId']);

        // Get definition
        $stopWatchId = 'get_definition';
        $stopWatch->start($stopWatchId);

        $definition = $this->getDefinition($definitionFile);

        $e = $stopWatch->stop($stopWatchId);
        $this->logEvent('Definition downloaded from s3', $job->getId(), $job->getRunId(), [
            'file' => $definitionFile
        ], $e->getDuration());

        $updateOperations = [];
        $ldmChange = false;
        try {
            // Update model
            $stopWatchId = 'GoodData';
            $stopWatch->start($stopWatchId);

            $result = $this->restApi->updateDataSet($params['pid'], $definition, $this->configuration->noDateFacts);
            if ($result) {
                $updateOperations[] = $result;
                $ldmChange = true;
            }

            if (empty($tableDefinition['isExported'])) {
                // Save export status to definition
                $this->configuration->updateDataSetDefinition($params['tableId'], 'isExported', 1);
            }

            $e = $stopWatch->stop($stopWatchId);
            $this->logEvent('LDM API called', $job->getId(), $job->getRunId(), [
                'operations' => $updateOperations
            ], $e->getDuration());

        } catch (\Exception $e) {
            $error = $e->getMessage();
            $event = $stopWatch->stop($stopWatchId);

            if ($e instanceof RestApiException) {
                $error = $e->getDetails();
            }
            $this->logEvent('Model update failed', $job->getId(), $job->getRunId(), [], $event->getDuration());

            if (!($e instanceof RestApiException)) {
                throw $e;
            }
        }

        $result = [];
        if (!empty($error)) {
            $result['error'] = $error;
        }
        if (count($updateOperations)) {
            $result['info'] = $updateOperations;
        }
        if ($ldmChange) {
            $result['ldmChange'] = true;  //@TODO remove after UI update
            $result['flags'] = ['ldm' => $this->translator->trans('result.flag.ldm')];
        }

        return $result;
    }
}
