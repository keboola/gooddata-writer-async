<?php
/**
 * @author Jakub Matejka <jakub@keboola.com>
 * @date 2013-04-17
 */

namespace Keboola\GoodDataWriter\Job;

use Keboola\GoodDataWriter\Exception\WrongConfigurationException,
	Keboola\GoodDataWriter\GoodData\CLToolApiErrorException,
	Keboola\GoodDataWriter\GoodData\RestApiException;
use Keboola\GoodDataWriter\GoodData\CLToolApi;
use Keboola\GoodDataWriter\GoodData\Model;
use Symfony\Component\Process\Process;
use Symfony\Component\Stopwatch\Stopwatch;

class UpdateModel extends AbstractJob
{
	private $goodDataModel;

	/**
	 * required: tableId
	 * optional: pid
	 */
	public function run($job, $params)
	{
		$this->checkParams($params, array('tableId'));

		$this->logEvent('start', array(
			'duration' => 0
		));
		$stopWatch = new Stopwatch();
		$stopWatchId = 'prepareJob';
		$stopWatch->start($stopWatchId);

		if (empty($job['definition'])) {
			throw new WrongConfigurationException("Definition for data set is missing. Try the upload again please.");
		}
		$bucketAttributes = $this->configuration->bucketAttributes();
		$this->configuration->checkBucketAttributes();
		$this->configuration->updateDataSetsFromSapi();

		// Init
		$tmpFolderName = basename($this->tmpDir);
		$this->goodDataModel = new Model($this->appConfiguration);
		$projects = $this->configuration->getProjects();

		$updateModelJobs = array();

		$tableDefinition = $this->configuration->getDataSet($params['tableId']);

		$this->restApi->login($bucketAttributes['gd']['username'], $bucketAttributes['gd']['password']);

		$e = $stopWatch->stop($stopWatchId);
		$this->logEvent($stopWatchId, array(
			'duration' => $e->getDuration()
		), $this->restApi->getLogPath());

		// Get definition
		$stopWatchId = 'getDefinition';
		$stopWatch->start($stopWatchId);
		$definitionFile = $job['definition'];

		$definitionUrl = $this->s3Client->url($definitionFile);
		$command = 'curl -sS -L --retry 12 ' . escapeshellarg($definitionUrl);
		$process = new Process($command);
		$process->setTimeout(null);
		$process->run();
		$error = $process->getErrorOutput();
		if (!$process->isSuccessful() || $error) {
			throw new \Exception($this->translator->trans('error.s3_download_fail') . ': ' . json_encode(array(
					'command' => $command,
					'error' => $error,
					'output' => $process->getOutput()
				)));
		}
		$definition = json_decode($process->getOutput(), true);
		if (!$definition) {
			throw new \Exception($this->translator->trans('error.s3_download_fail') . ': ' . json_encode(array(
					'command' => $command,
					'error' => $error,
					'output' => $process->getOutput()
				)));
		}

		$e = $stopWatch->stop($stopWatchId);
		$this->logEvent($stopWatchId, array(
			'duration' => $e->getDuration(),
			'definition' => $definitionUrl
		));

		$dataSetName = !empty($tableDefinition['name']) ? $tableDefinition['name'] : $tableDefinition['id'];
		$dataSetId = Model::getDatasetId($dataSetName);




		$stopWatchId = 'prepareLoads';
		$stopWatch->start($stopWatchId);
		// Choose projects to load
		$projectsToLoad = array();
		foreach ($projects as $project) if ($project['active']) {
			if (in_array($project['pid'], array_keys($projectsToLoad))) {
				throw new WrongConfigurationException($this->translator->trans('configuration.project.duplicated %1', array('%1' => $project['pid'])));
			}

			if (!isset($params['pid']) || $project['pid'] == $params['pid']) {
				$projectsToLoad[$project['pid']] = array(
					'pid' => $project['pid'],
					'main' => !empty($project['main']),
					'existingDataSets' => $this->restApi->getDataSets($project['pid'])
				);
			}
		}
		if (isset($params['pid']) && !count($projectsToLoad)) {
			throw new WrongConfigurationException($this->translator->trans('parameters.pid_not_configured'));
		}


		// Enqueue jobs for creation/update of dataSet
		$modelChangeDecisionsLog = array();
		foreach ($projectsToLoad as $project) {
			$dataSetExists = in_array($dataSetId, array_keys($project['existingDataSets']));
			$lastGoodDataUpdate = empty($project['existingDataSets'][$dataSetId]['lastChangeDate'])? null : Model::getTimestampFromApiDate($project['existingDataSets'][$dataSetId]['lastChangeDate']);

			$lastConfigurationUpdate = empty($tableDefinition['lastChangeDate'])? null : strtotime($tableDefinition['lastChangeDate']);
			$doUpdate = $dataSetExists && $lastConfigurationUpdate && (!$lastGoodDataUpdate || $lastGoodDataUpdate < $lastConfigurationUpdate);

			if ($dataSetExists) {
				if ($doUpdate) {
					$updateModelJobs[] = array(
						'command' => 'update',
						'pid' => $project['pid'],
						'mainProject' => !empty($project['main'])
					);
				}
			} else {
				$updateModelJobs[] = array(
					'command' => 'create',
					'pid' => $project['pid'],
					'mainProject' => !empty($project['main'])
				);
			}

			$modelChangeDecisionsLog[$project['pid']] = array(
				'dataSetExists' => $dataSetExists,
				'lastGoodDataUpdate' => $lastGoodDataUpdate . ($lastGoodDataUpdate? ' - ' . strtotime($lastGoodDataUpdate) : null),
				'lastConfigurationUpdate' => $lastConfigurationUpdate . ($lastConfigurationUpdate? ' - ' . strtotime($lastConfigurationUpdate) : null),
				'doUpdate' => $doUpdate
			);
		}


		//@TODO REMOVE WITH CL TOOL
		$clToolApi = null;
		if (!$this->preRelease) {
			$clToolApi = new CLToolApi($this->logger, $this->appConfiguration->clPath);
			$clToolApi->s3client = $this->s3Client;
			$clToolApi->setCredentials($bucketAttributes['gd']['username'], $bucketAttributes['gd']['password']);
		}
		//@TODO REMOVE WITH CL TOOL

		$e = $stopWatch->stop($stopWatchId);
		$this->logEvent($stopWatchId, array(
			'duration' => $e->getDuration(),
			'modelChangeDecisions' => $modelChangeDecisionsLog
		));

		$gdWriteStartTime = date('c');
		$updateOperations = array();
		try {
			// Update model
			foreach ($updateModelJobs as $gdJob) {
				$stopWatchId = $gdJob['command'] . 'DataSet'.'-'.$gdJob['pid'];
				$stopWatch->start($stopWatchId);

				if ($this->preRelease) {
					$this->restApi->initLog();

					$result = $this->restApi->updateDataSet($gdJob['pid'], $definition);
					if ($result) {
						$updateOperations[$gdJob['pid']] = $result;
					}

					if (empty($tableDefinition['isExported'])) {
						// Save export status to definition
						$this->configuration->updateDataSetDefinition($params['tableId'], 'isExported', 1);
					}

					$e = $stopWatch->stop($stopWatchId);
					$this->logEvent($stopWatchId, array(
						'duration' => $e->getDuration()
					), $this->restApi->getLogPath());

					//@TODO REMOVE WITH CL TOOL
				} else {
					$clToolApi->debugLogUrl = null;
					$this->restApi->initLog();
					$clToolApi->s3Dir = $tmpFolderName . '/' . $gdJob['pid'];
					$clToolApi->tmpDir = $this->tmpDir . '/' . $gdJob['pid'];
					if (!file_exists($clToolApi->tmpDir)) mkdir($clToolApi->tmpDir);
					$xml = CLToolApi::getXml($definition);
					if ($gdJob['command'] == 'create') {
						$maql = $clToolApi->createDataSetMaql($gdJob['pid'], $xml, $dataSetName);
						$this->restApi->executeMaql($gdJob['pid'], $maql);

						if (empty($tableDefinition['isExported'])) {
							// Save export status to definition
							$this->configuration->updateDataSetDefinition($params['tableId'], 'isExported', 1);
						}
					} else {
						$maql = $clToolApi->updateDataSetMaql($gdJob['pid'], $xml, 1, $dataSetName);
						if ($maql) {
							$this->restApi->executeMaql($gdJob['pid'], $maql);
						}
					}
					if ($clToolApi->debugLogUrl) {
						if ($gdJob['mainProject']) {
							$this->logs['CL ' . $gdJob['command'] . ' DataSet'] = $clToolApi->debugLogUrl;
						} else {
							$this->logs[$gdJob['pid']]['CL ' . $gdJob['command'] . ' DataSet'] = $clToolApi->debugLogUrl;
						}
						$clToolApi->debugLogUrl = null;
					}
					$e = $stopWatch->stop($stopWatchId);
					$this->logEvent($stopWatchId, array(
						'duration' => $e->getDuration(),
						'xml' => $xml,
						'clTool' => $clToolApi->output
					), $this->restApi->getLogPath());
				}
				//@TODO REMOVE WITH CL TOOL

			}
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
				$restApiLogPath = $this->restApi->getLogPath();
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

		return $result;
	}

}
