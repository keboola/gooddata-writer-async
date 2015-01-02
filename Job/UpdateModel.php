<?php
/**
 * @author Jakub Matejka <jakub@keboola.com>
 * @date 2013-04-17
 */

namespace Keboola\GoodDataWriter\Job;

use Keboola\GoodDataWriter\Exception\WrongConfigurationException,
	Keboola\GoodDataWriter\GoodData\RestApiException;
use Keboola\GoodDataWriter\Exception\WrongParametersException;
use Keboola\GoodDataWriter\GoodData\Model;
use Keboola\GoodDataWriter\GoodData\RestApi;
use Symfony\Component\Stopwatch\Stopwatch;

class UpdateModel extends AbstractJob
{

	public function prepare($params)
	{
		$this->checkParams($params, array('writerId', 'tableId'));
		$this->checkWriterExistence($params['writerId']);
		$this->configuration->checkBucketAttributes();

		return array(
			'tableId' => $params['tableId']
		);
	}

	/**
	 * required: pid, tableId
	 * optional:
	 */
	public function run($job, $params, RestApi $restApi)
	{
		$this->checkParams($params, array('pid', 'tableId'));
		$project = $this->configuration->getProject($params['pid']);
		if (!$project) {
			throw new WrongParametersException($this->translator->trans('parameters.pid_not_configured'));
		}
		if (empty($job['definition'])) {
			throw new WrongConfigurationException($this->translator->trans('job_executor.data_set_definition_missing'));
		}

		$bucketAttributes = $this->configuration->bucketAttributes();
		$this->configuration->checkBucketAttributes($bucketAttributes);
		$this->configuration->updateDataSetsFromSapi();

		$stopWatch = new Stopwatch();


		// Init
		$restApi->login($bucketAttributes['gd']['username'], $bucketAttributes['gd']['password']);

		$tableDefinition = $this->configuration->getDataSet($params['tableId']);

		// Get definition
		$stopWatchId = 'get_definition';
		$stopWatch->start($stopWatchId);
		$definitionFile = $job['definition'];

		$definition = $this->s3Client->downloadFile($definitionFile);
		$definition = json_decode($definition, true);
		if (!$definition) {
			throw new \Exception($this->translator->trans('error.s3_download_fail') . ': ' . $definitionFile);
		}

		$e = $stopWatch->stop($stopWatchId);
		$this->logEvent('Definition downloaded from s3', $job['id'], $job['runId'], array(
			'file' => $definitionFile
		), $e->getDuration());

		$dataSetName = !empty($tableDefinition['name']) ? $tableDefinition['name'] : $tableDefinition['id'];


		$updateOperations = array();
		$ldmChange = false;
		try {
			// Update model
			$stopWatchId = 'GoodData';
			$stopWatch->start($stopWatchId);

			$result = $restApi->updateDataSet($params['pid'], $definition, $this->configuration->noDateFacts);
			if ($result) {
				$updateOperations[] = $result;
				$ldmChange = true;
			}

			if (empty($tableDefinition['isExported'])) {
				// Save export status to definition
				$this->configuration->updateDataSetDefinition($params['tableId'], 'isExported', 1);
			}

			$e = $stopWatch->stop($stopWatchId);
			$this->logEvent('LDM API called', $job['id'], $job['runId'], array(
				'operations' => $updateOperations
			), $e->getDuration());

		} catch (\Exception $e) {
			$error = $e->getMessage();
			$event = $stopWatch->stop($stopWatchId);

			$restApiLogPath = null;
			if ($e instanceof RestApiException) {
				$error = $e->getDetails();
			}
			$this->logEvent('Model update failed', $job['id'], $job['runId'], array(), $event->getDuration());

			if (!($e instanceof RestApiException)) {
				throw $e;
			}
		}

		$result = array();
		if (!empty($error)) {
			$result['error'] = $error;
		}
		if (count($updateOperations)) {
			$result['info'] = $updateOperations;
		}
		if ($ldmChange) {
			$result['ldmChange'] = true;  //@TODO remove after UI update
			$result['flags'] = array('ldm' => $this->translator->trans('result.flag.ldm'));
		}

		return $result;
	}

}
