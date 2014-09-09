<?php
/**
 * @author Jakub Matejka <jakub@keboola.com>
 * @date 2013-04-17
 */

namespace Keboola\GoodDataWriter\Job;

use Keboola\GoodDataWriter\Exception\WrongConfigurationException,
	Keboola\GoodDataWriter\GoodData\CLToolApiErrorException,
	Keboola\GoodDataWriter\GoodData\RestApiException;
use Keboola\GoodDataWriter\Exception\WrongParametersException;
use Keboola\GoodDataWriter\GoodData\CLToolApi;
use Keboola\GoodDataWriter\GoodData\Model;
use Keboola\GoodDataWriter\GoodData\RestApi;
use Symfony\Component\Stopwatch\Stopwatch;

class UpdateModel extends AbstractJob
{
	private $goodDataModel;

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

		$this->logEvent('start', array(
			'duration' => 0
		));
		$stopWatch = new Stopwatch();


		// Init
		$tmpFolderName = basename($this->getTmpDir($job['id']));
		$this->goodDataModel = new Model($this->appConfiguration);
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
		$this->logEvent($stopWatchId, array(
			'duration' => $e->getDuration(),
			'definition' => $definitionFile
		));

		$dataSetName = !empty($tableDefinition['name']) ? $tableDefinition['name'] : $tableDefinition['id'];
		$dataSetId = Model::getDatasetId($dataSetName);


		//@TODO REMOVE WITH CL TOOL
		$clToolApi = null;
		if ($this->configuration->clTool) {
			$clToolApi = new CLToolApi($this->logger, $this->appConfiguration->clPath);
			$clToolApi->s3client = $this->s3Client;
			$clToolApi->setCredentials($bucketAttributes['gd']['username'], $bucketAttributes['gd']['password']);
		}
		//@TODO REMOVE WITH CL TOOL


		$gdWriteStartTime = date('c');
		$updateOperations = array();
		$ldmChange = false;
		try {
			// Update model
			$stopWatchId = 'GoodData';
			$stopWatch->start($stopWatchId);

			if (!$this->configuration->clTool) {
				$restApi->initLog();

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
				$this->logEvent($stopWatchId, array(
					'duration' => $e->getDuration()
				), $restApi->getLogPath());

			//@TODO REMOVE WITH CL TOOL
			} else {
				$clToolApi->debugLogUrl = null;
				$restApi->initLog();
				$clToolApi->s3Dir = $tmpFolderName;
				$clToolApi->tmpDir = $this->getTmpDir($job['id']);
				if (!file_exists($clToolApi->tmpDir)) mkdir($clToolApi->tmpDir);
				$xml = CLToolApi::getXml($definition);

				$existingDataSets = $restApi->getDataSets($params['pid']);
				$dataSetExists = in_array($dataSetId, array_keys($existingDataSets));

				$maql = $dataSetExists? $clToolApi->updateDataSetMaql($params['pid'], $xml, 1, $dataSetName)
					: $clToolApi->createDataSetMaql($params['pid'], $xml, $dataSetName);
				if ($maql) {
					$restApi->executeMaql($params['pid'], $maql);
					$ldmChange = true;
				}
				if (empty($tableDefinition['isExported'])) {
					// Save export status to definition
					$this->configuration->updateDataSetDefinition($params['tableId'], 'isExported', 1);
				}
				if ($clToolApi->debugLogUrl) {
					$this->logs['CL Tool'] = $clToolApi->debugLogUrl;
					$clToolApi->debugLogUrl = null;
				}
				$e = $stopWatch->stop($stopWatchId);
				$this->logEvent($stopWatchId, array(
					'duration' => $e->getDuration(),
					'xml' => $xml,
					'clTool' => $clToolApi->output
				), $restApi->getLogPath());
			}
			//@TODO REMOVE WITH CL TOOL

		} catch (\Exception $e) {
			$error = $e->getMessage();
			$event = $stopWatch->stop($stopWatchId);

			$restApiLogPath = null;
			$eventDetails = array(
				'duration' => $event->getDuration()
			);
			if ($e instanceof CLToolApiErrorException) {
				if ($clToolApi->debugLogUrl) {
					$this->logs['CL Tool Error'] = $clToolApi->debugLogUrl;
					$clToolApi->debugLogUrl = null;
				}
				$eventDetails['clTool'] = $clToolApi->output;
				$data = $e->getData();
				if (count($data)) {
					$this->logs['CL Tool Debug'] = $this->s3Client->uploadString($job['id'] . '/debug-data.json', json_encode($data));
				}
			} elseif ($e instanceof RestApiException) {
				$error = $e->getDetails();
				$restApiLogPath = $restApi->getLogPath();
			}
			$this->logEvent($stopWatchId, $eventDetails, $restApiLogPath);

			if (!($e instanceof CLToolApiErrorException) && !($e instanceof RestApiException)) {
				throw $e;
			}
		}

		$result = array();
		if (!empty($error)) {
			$result['error'] = $error;
		}
		if (!empty($gdWriteStartTime)) {
			$result['gdWriteStartTime'] = $gdWriteStartTime;
		}
		if (count($updateOperations)) {
			$result['info'] = $updateOperations;
		}
		if ($ldmChange) {
			$result['ldmChange'] = true;
		}

		return $result;
	}

}
