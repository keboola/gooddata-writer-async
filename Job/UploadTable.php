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

	public function run($job, $params)
	{
		$this->logEvent('start', array(
			'duration' => 0
		));
		$stopWatch = new Stopwatch();
		$stopWatchId = 'prepareJob';
		$stopWatch->start($stopWatchId);

		if (empty($job['xmlFile'])) {
			throw new WrongConfigurationException("Parameter 'xmlFile' is missing");
		}
		$bucketAttributes = $this->configuration->bucketAttributes();
		$this->configuration->checkBucketAttributes();
		$this->configuration->updateDataSetsFromSapi();

		// Init
		$tmpFolderName = basename($this->tmpDir);
		$this->goodDataModel = new Model($this->appConfiguration);
		$this->csvHandler = new CsvHandler($this->scriptsPath, $this->s3Client, $this->tmpDir, $this->logger);
		$this->csvHandler->setJobId($job['id']);
		$this->csvHandler->setRunId($job['runId']);
		$projects = $this->configuration->getProjects();
		$debug = array();

		$updateModelJobs = array();
		$loadDataJobs = array();

		$tableDefinition = $this->configuration->getDataSet($params['tableId']);
		$incrementalLoad = (isset($params['incrementalLoad'])) ? $params['incrementalLoad']
			: (!empty($tableDefinition['incrementalLoad']) ? $tableDefinition['incrementalLoad'] : 0);
		$filterColumn = $this->getFilterColumn($params['tableId'], $tableDefinition, $bucketAttributes);
		$ldmChange = false;

		$webDavUrl = $this->getWebDavUrl($bucketAttributes);
		$this->restApi->login($bucketAttributes['gd']['username'], $bucketAttributes['gd']['password']);

		$e = $stopWatch->stop($stopWatchId);
		$this->logEvent($stopWatchId, array(
			'duration' => $e->getDuration()
		), $this->restApi->getLogPath());

		// Get definition
		$stopWatchId = 'getDefinition';
		$stopWatch->start($stopWatchId);
		$definitionFile = $job['xmlFile'];

		$definitionUrl = $this->s3Client->url($definitionFile);
		$command = 'curl -sS -L --retry 12 ' . escapeshellarg($definitionUrl);
		$process = new Process($command);
		$process->setTimeout(null);
		$process->run();
		$error = $process->getErrorOutput();
		if (!$process->isSuccessful() || $error) {
			$e = new JobProcessException('Definition download from S3 failed.');
			$e->setData(array(
				'command' => $command,
				'error' => $error,
				'output' => $process->getOutput()
			));
			throw $e;
		}
		$definition = json_decode($process->getOutput(), true);
		if (!$definition) {
			$e = new JobProcessException('Definition download from S3 failed.');
			$e->setData(array(
				'command' => $command,
				'error' => $error,
				'output' => $process->getOutput()
			));
			throw $e;
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
		$debug['manifest'] = $this->s3Client->uploadFile($this->tmpDir . '/upload_info.json', 'text/plain', $tmpFolderName . '/manifest.json');
		$e = $stopWatch->stop($stopWatchId);
		$this->logEvent($stopWatchId, array(
			'duration' => $e->getDuration(),
			'manifest' => $debug['manifest']
		));


		$stopWatchId = 'prepareLoads';
		$stopWatch->start($stopWatchId);
		// Choose projects to load
		$projectsToLoad = array();
		foreach ($projects as $project) if ($project['active']) {
			if (in_array($project['pid'], array_keys($projectsToLoad))) {
				throw new WrongConfigurationException("Project '" . $project['pid'] . "' is duplicated in configuration");
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
			throw new WrongConfigurationException("Project '" . $params['pid'] . "' was not found in configuration");
		}


		// Enqueue jobs for creation/update of dataSet and load data
		foreach ($projectsToLoad as $project) {
			$dataSetExists = in_array($dataSetId, array_keys($project['existingDataSets']));
			if ($dataSetExists) {
				if (!empty($tableDefinition['lastChangeDate'])
					&& (empty($project['existingDataSets'][$dataSetId]['lastChangeDate'])
					|| strtotime($project['existingDataSets'][$dataSetId]['lastChangeDate']) < strtotime($tableDefinition['lastChangeDate']))) {

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
		}


		$clToolApi = null;
		if (!$this->preRelease) {
			$clToolApi = new CLToolApi($this->logger, $this->appConfiguration->clPath);
			$clToolApi->s3client = $this->s3Client;
			if (isset($bucketAttributes['gd']['backendUrl'])) {
				$urlParts = parse_url($bucketAttributes['gd']['backendUrl']);
				if ($urlParts && !empty($urlParts['host'])) {
					$clToolApi->setBackendUrl($urlParts['host']);
				}
			}
			$clToolApi->setCredentials($bucketAttributes['gd']['username'], $bucketAttributes['gd']['password']);
		}
		$e = $stopWatch->stop($stopWatchId);
		$this->logEvent($stopWatchId, array(
			'duration' => $e->getDuration()
		));



		try {
			// Update model
			foreach ($updateModelJobs as $gdJob) {
				$stopWatchId = $gdJob['command'] . 'DataSet'.'-'.$gdJob['pid'];
				$stopWatch->start($stopWatchId);

				if ($this->preRelease) {
					$this->restApi->initLog();

					$this->restApi->updateDataSet($gdJob['pid'], $definition);

					$e = $stopWatch->stop($stopWatchId);
					$this->logEvent($stopWatchId, array(
						'duration' => $e->getDuration()
					), $this->restApi->getLogPath());
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
							$debug[$gdJob['command'] . ' dataset'] = $clToolApi->debugLogUrl;
						} else {
							$debug[$gdJob['pid']][$gdJob['command'] . ' dataset'] = $clToolApi->debugLogUrl;
						}
						$clToolApi->debugLogUrl = null;
					}
					$e = $stopWatch->stop($stopWatchId);
					$this->logEvent($stopWatchId, array(
						'duration' => $e->getDuration(),
						'clTool' => $clToolApi->output
					), $this->restApi->getLogPath());
				}

				$ldmChange = true;
			}



			// Upload to WebDav
			$webDav = new WebDav($bucketAttributes['gd']['username'], $bucketAttributes['gd']['password'], $webDavUrl);
			if (!$webDavUrl) $webDavUrl = $webDav->url;

			// Upload dataSets
			foreach ($loadDataJobs as $gdJob) {
				$stopWatchId = 'transferCsv-' . $gdJob['pid'];
				$stopWatch->start($stopWatchId);
				$webDavFolder = $tmpFolderName . '-' . $gdJob['pid'];

				$webDav->prepareFolder($webDavFolder);

				$this->csvHandler->initDownload($params['tableId'], $job['token'], $this->appConfiguration->storageApiUrl,
					$this->appConfiguration->userAgent, $gdJob['incrementalLoad'], $gdJob['filterColumn'], $gdJob['pid']);
				$this->csvHandler->prepareTransformation($definition);
				$this->csvHandler->runUpload($bucketAttributes['gd']['username'], $bucketAttributes['gd']['password'], $webDavUrl, '/uploads/' . $webDavFolder);
				if (!$webDav->fileExists($webDavFolder . '/data.csv')) {
					throw new WebDavException(sprintf("Csv file has not been uploaded to '%s/uploads/%s/data.csv'", $webDavUrl, $webDavFolder));
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
					$this->restApi->loadData($gdJob['pid'], $webDavFolder, Model::getId($dataSetName));
				} catch (RestApiException $e) {
					$debugFile = $dataTmpDir . '/etl.log';
					$taskName = 'Data load';
					$logSaved = $webDav->saveLogs($webDavFolder, $debugFile);
					if ($logSaved) {
						$logUrl = $this->s3Client->uploadFile($debugFile, 'text/plain', sprintf('%s/%s/%s-etl.log', $tmpFolderName, $gdJob['pid'], $dataSetName));
						if ($gdJob['mainProject']) {
							$debug[$taskName] = $logUrl;
						} else {
							$debug[$gdJob['pid']][$taskName] = $logUrl;
						}
					}

					throw new RestApiException('ETL load failed', $e->getMessage());
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
					$debug[(count($debug) + 1) . ': CL tool'] = $clToolApi->debugLogUrl;
					$clToolApi->debugLogUrl = null;
				}
				$eventDetails['clTool'] = $clToolApi->output;
				$data = $e->getData();
				if (count($data)) {
					$debug[(count($debug) + 1) . ': debug data'] = $this->s3Client->uploadString($job['id'] . '/debug-data.json', json_encode($data));
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
			'debug' => $debug,
			'incrementalLoad' => (int) $incrementalLoad,
			'ldmChange' => (bool) $ldmChange,
		);
		if (!empty($error)) {
			$result['error'] = $error;
		}
		if (!empty($gdWriteStartTime)) {
			$result['gdWriteStartTime'] = $gdWriteStartTime;
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
				throw new WrongConfigurationException("Filter column does not exist in the table");
			}
			if (!in_array($filterColumn, $tableInfo['indexedColumns'])) {
				throw new WrongConfigurationException("Filter column does not have index");
			}
		}
		return $filterColumn;
	}

}
