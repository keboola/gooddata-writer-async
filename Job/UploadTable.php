<?php
/**
 * @author Jakub Matejka <jakub@keboola.com>
 * @date 2013-04-17
 */

namespace Keboola\GoodDataWriter\Job;

use Keboola\GoodDataWriter\Exception\WrongConfigurationException,
	Keboola\GoodDataWriter\Exception\JobProcessException,
	Keboola\GoodDataWriter\GoodData\CLToolApiErrorException,
	Keboola\GoodDataWriter\GoodData\RestApiException;
use Keboola\GoodDataWriter\GoodData\CLToolApi,
	Keboola\GoodDataWriter\GoodData\CsvHandler,
	Keboola\GoodDataWriter\GoodData\WebDav;
use Keboola\GoodDataWriter\GoodData\Model;
use Keboola\GoodDataWriter\GoodData\WebDavException;
use Symfony\Component\Process\Process;
use Symfony\Component\Stopwatch\Stopwatch;

class UploadTable extends AbstractJob
{
	/**
	 * @var CsvHandler
	 */
	private $csvHandler;
	private $goodDataModel;

	/**
	 * required: tableId
	 * optional: incrementalLoad, pid
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
		$this->csvHandler = new CsvHandler($this->appConfiguration->scriptsPath, $this->storageApiClient);
		$this->csvHandler->setJobId($job['id']);
		$this->csvHandler->setRunId($job['runId']);
		$projects = $this->configuration->getProjects();

		$updateModelJobs = array();
		$loadDataJobs = array();

		$tableDefinition = $this->configuration->getDataSet($params['tableId']);
		$incrementalLoad = (isset($params['incrementalLoad'])) ? $params['incrementalLoad']
			: (!empty($tableDefinition['incrementalLoad']) ? $tableDefinition['incrementalLoad'] : 0);
		$filterColumn = $this->getFilterColumn($params['tableId'], $tableDefinition, $bucketAttributes);
		$ldmChange = false;

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


		// Get manifest
		$stopWatchId = 'getManifest';
		$stopWatch->start($stopWatchId);
		$manifest = Model::getDataLoadManifest($definition, $incrementalLoad);
		file_put_contents($this->tmpDir . '/upload_info.json', json_encode($manifest));
		$this->logs['Manifest'] = $this->s3Client->uploadFile($this->tmpDir . '/upload_info.json', 'text/plain', $tmpFolderName . '/manifest.json');
		$e = $stopWatch->stop($stopWatchId);
		$this->logEvent($stopWatchId, array(
			'duration' => $e->getDuration(),
			'manifest' => $this->logs['Manifest']
		));


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


		// Enqueue jobs for creation/update of dataSet and load data
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

			$loadDataJobs[] = array(
				'command' => 'loadData',
				'pid' => $project['pid'],
				'filterColumn' => ($filterColumn && empty($project['main'])) ? $filterColumn : false,
				'mainProject' => !empty($project['main']),
				'incrementalLoad' => ($dataSetExists && $incrementalLoad) ? $incrementalLoad : 0
			);

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

				$ldmChange = true;
			}



			// Upload to WebDav
			$webDav = new WebDav($bucketAttributes['gd']['username'], $bucketAttributes['gd']['password']);
			$webDavUrl = $webDav->url;

			// Upload dataSets
			foreach ($loadDataJobs as $gdJob) {
				$stopWatchId = 'transferCsv-' . $gdJob['pid'];
				$stopWatch->start($stopWatchId);
				$webDavFolder = $tmpFolderName . '-' . $gdJob['pid'];

				$webDav->prepareFolder($webDavFolder);

				$this->csvHandler->initDownload($params['tableId'], $gdJob['incrementalLoad'], $gdJob['filterColumn'], $gdJob['pid']);
				$this->csvHandler->prepareTransformation($definition);
				$this->csvHandler->runUpload($bucketAttributes['gd']['username'], $bucketAttributes['gd']['password'], $webDavUrl, '/uploads/' . $webDavFolder);
				if (!$webDav->fileExists($webDavFolder . '/data.csv')) {
					throw new WrongConfigurationException($this->translator->trans('error.csv_not_uploaded %1', array('%1' => sprintf('%s/uploads/%s/data.csv', $webDavUrl, $webDavFolder))));
				}

				$e = $stopWatch->stop($stopWatchId);
				$this->logEvent($stopWatchId, array(
					'duration' => $e->getDuration()
				));

				$stopWatchId = 'uploadManifest-' . $gdJob['pid'];
				$stopWatch->start($stopWatchId);

				$webDav->upload($this->tmpDir . '/upload_info.json', $webDavFolder);
				$e = $stopWatch->stop($stopWatchId);
				$this->logEvent($stopWatchId, array(
					'duration' => $e->getDuration(),
					'url' => $webDavUrl,
					'folder' => '/uploads/' . $webDavFolder
				));
			}



			// Run ETL tasks
			$gdWriteStartTime = date('c');

			// Run ETL task of dataSets
			foreach ($loadDataJobs as $gdJob) {
				$stopWatchId = 'runEtl-' . $gdJob['pid'];
				$stopWatch->start($stopWatchId);
				$this->restApi->initLog();
				$dataTmpDir = $this->tmpDir . '/' . $gdJob['pid'];
				if (!file_exists($dataTmpDir)) mkdir($dataTmpDir);
				$webDavFolder = $tmpFolderName . '-' . $gdJob['pid'];

				try {
					$this->restApi->loadData($gdJob['pid'], $webDavFolder);
				} catch (RestApiException $e) {
					$debugFile = $dataTmpDir . '/etl.log';
					$taskName = 'Data Load Error';
					$logSaved = $webDav->saveLogs($webDavFolder, $debugFile);
					if ($logSaved) {
						if (filesize($debugFile) > 1024 * 1024) {
							$logUrl = $this->s3Client->uploadFile($debugFile, 'text/plain', sprintf('%s/%s/%s-etl.log', $tmpFolderName, $gdJob['pid'], $dataSetName));
							if ($gdJob['mainProject']) {
								$this->logs[$taskName] = $logUrl;
							} else {
								$this->logs[$gdJob['pid']][$taskName] = $logUrl;
							}
							$e->setDetails(array($logUrl));
						} else {
							$e->setDetails(file_get_contents($debugFile));
						}
					}

					throw $e;
				}
				$e = $stopWatch->stop($stopWatchId);
				$this->logEvent($stopWatchId, array(
					'duration' => $e->getDuration()
				), $this->restApi->getLogPath());
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

			if (!($e instanceof CLToolApiErrorException) && !($e instanceof RestApiException) && !($e instanceof WebDavException)) {
				throw $e;
			}
		}

		$result = array(
			'incrementalLoad' => (int) $incrementalLoad,
			'ldmChange' => (bool) $ldmChange,
		);
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


	private function getFilterColumn($tableId, $tableDefinition, $bucketAttributes)
	{
		$filterColumn = null;
		if (isset($bucketAttributes['filterColumn']) && empty($tableDefinition['ignoreFilter'])) {
			$filterColumn = $bucketAttributes['filterColumn'];
			$tableInfo = $this->configuration->getSapiTable($tableId);
			if (!in_array($filterColumn, $tableInfo['columns'])) {
				throw new WrongConfigurationException($this->translator->trans('configuration.upload.filter_missing'));
			}
			if (!in_array($filterColumn, $tableInfo['indexedColumns'])) {
				throw new WrongConfigurationException($this->translator->trans('configuration.upload.filter_index_missing'));
			}
		}
		return $filterColumn;
	}

}
