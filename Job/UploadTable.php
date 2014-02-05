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
	Keboola\GoodDataWriter\GoodData\RestApi,
	Keboola\GoodDataWriter\GoodData\CsvHandler,
	Keboola\GoodDataWriter\GoodData\CsvHandlerException,
	Keboola\GoodDataWriter\GoodData\WebDav;
use Keboola\GoodDataWriter\GoodData\Model;
use Keboola\GoodDataWriter\GoodData\WebDavException;
use Keboola\GoodDataWriter\Exception\ClientException;
use Symfony\Component\Process\Process;
use Symfony\Component\Stopwatch\Stopwatch;

class UploadTable extends AbstractJob
{
	/**
	 * @var CsvHandler
	 */
	private $csvHandler;
	private $goodDataModel;
	public $eventsLog;

	public function run($job, $params)
	{
		$this->eventsLog = array();
		$this->eventsLog['start'] = array('duration' => 0, 'time' => date('c'));
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
		$this->goodDataModel = new Model($this->scriptsPath);
		$this->csvHandler = new CsvHandler($this->scriptsPath, $this->s3Client, $this->tmpDir, $job['id']);
		$projects = $this->configuration->getProjects();
		$error = false;
		$debug = array();

		$createDateJobs = array();
		$updateModelJobs = array();
		$loadDataJobs = array();

		$tableDefinition = $this->configuration->getDataSet($params['tableId']);
		$incrementalLoad = (isset($params['incrementalLoad'])) ? $params['incrementalLoad']
			: (!empty($tableDefinition['incrementalLoad']) ? $tableDefinition['incrementalLoad'] : 0);
		$filterColumn = $this->getFilterColumn($params['tableId'], $tableDefinition, $bucketAttributes);

		$webDavUrl = $this->getWebDavUrl($bucketAttributes);
		$this->restApi->login($bucketAttributes['gd']['username'], $bucketAttributes['gd']['password']);

		$e = $stopWatch->stop($stopWatchId);
		$this->eventsLog[$stopWatchId] = array(
			'duration' => $e->getDuration(),
			'time' => date('c'),
			'restApi' => $this->restApi->callsLog
		);
		$this->restApi->callsLog = array();

		// Get definition
		$stopWatchId = 'getDefinition';
		$stopWatch->start($stopWatchId);
		$definitionFile = $job['xmlFile'];
		try {
			$definitionUrl = $this->s3Client->url($definitionFile);

			$command = 'curl -sS -L --retry 12 ' . escapeshellarg($definitionUrl);
			$process = new Process($command);
			$process->setTimeout(null);
			$process->run();
			if (!$process->isSuccessful()) {
				$e = new ClientException('Definition download from S3 failed.');
				$e->setData(array(
					'command' => $command,
					'error' => $process->getErrorOutput(),
					'output' => $process->getOutput()
				));
				throw $e;
			}
			$definition = json_decode($process->getOutput(), true);

		} catch (CsvHandlerException $e) {
			$this->log->warn('Download of definition failed', array(
				'exception' => $e->getMessage(),
				'trace' => $e->getTraceAsString(),
				'job' => $job,
				'params' => $params
			));
			throw new JobProcessException('Download of definition failed');
		}
		$e = $stopWatch->stop($stopWatchId);
		$this->eventsLog[$stopWatchId] = array('duration' => $e->getDuration(), 'time' => date('c'), 'definition' => $definitionUrl);

		$dataSetName = !empty($tableDefinition['name']) ? $tableDefinition['name'] : $tableDefinition['id'];
		$dataSetId = Model::getDatasetId($dataSetName);


		// Get manifest
		$stopWatchId = 'getManifest';
		$stopWatch->start($stopWatchId);
		$manifest = Model::getDataLoadManifest($definition, $incrementalLoad);
		file_put_contents($this->tmpDir . '/upload_info.json', json_encode($manifest));
		$debug['manifest'] = $this->s3Client->uploadFile($this->tmpDir . '/upload_info.json', 'text/plain', $tmpFolderName . '/manifest.json');
		$e = $stopWatch->stop($stopWatchId);
		$this->eventsLog[$stopWatchId] = array('duration' => $e->getDuration(), 'time' => date('c'), 'manifest' => $debug['manifest']);


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


		// Find out new date dimensions and enqueue them for creation
		$dateDimensions = null;
		$dateDimensionsToLoad = array();
		$newDimensions = array();
		if ($definition['columns']) foreach ($definition['columns'] as $column) if ($column['type'] == 'DATE') {
			if (!$dateDimensions) {
				$dateDimensions = $this->configuration->getDateDimensions();
			}

			$dimension = $column['schemaReference'];
			if (!isset($dateDimensions[$dimension])) {
				throw new WrongConfigurationException("Date dimension '$dimension' does not exist");
			}

			if (!in_array($dimension, array_keys($dateDimensionsToLoad))) {
				$dateDimensionsToLoad[$dimension] = array(
					'name' => $dimension,
					'gdName' => Model::getId($dimension),
					'includeTime' => !empty($dateDimensions[$dimension]['includeTime'])
				);
				if (!$dateDimensions[$dimension]['isExported']) {
					$newDimensions[$dimension] = null;
				}
			}
		}
		$newDimensions = array_keys($newDimensions);

		// Enqueue jobs for creation/update of dataSet and load data
		foreach ($projectsToLoad as $project) {
			foreach ($dateDimensionsToLoad as $dimension) {
				if (!in_array(Model::getDimensionId($dimension['gdName']), array_keys($project['existingDataSets']))) {
					$createDateJobs[] = array(
						'pid' => $project['pid'],
						'name' => $dimension['name'],
						'includeTime' => $dimension['includeTime'],
						'mainProject' => !empty($project['main'])
					);
				}
			}

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
			$clPath = null;
			if (!empty($this->mainConfig['cl_path'])) {
				$clPath = $this->mainConfig['cl_path'];
			}
			$clToolApi = new CLToolApi($this->log, $clPath);
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
		$this->eventsLog[$stopWatchId] = array('duration' => $e->getDuration(), 'time' => date('c'));



		try {

			// Create date dimensions
			foreach ($createDateJobs as $gdJob) {
				$stopWatchId = 'createDimension-' . $gdJob['name'] . '-' . $gdJob['pid'];
				$stopWatch->start($stopWatchId);
				$this->restApi->callsLog = array();
				$this->restApi->createDateDimension($gdJob['pid'], $gdJob['name'], $gdJob['includeTime']);

				if (in_array($gdJob['name'], $newDimensions)) {
					// Save export status to definition
					$this->configuration->setDateDimensionIsExported($gdJob['name']);
					unset($newDimensions[$gdJob['name']]);
				}

				$e = $stopWatch->stop($stopWatchId);
				$this->eventsLog[$stopWatchId] = array(
					'duration' => $e->getDuration(),
					'time' => date('c'),
					'restApi' => $this->restApi->callsLog
				);
			}


			// Update model
			foreach ($updateModelJobs as $gdJob) {
				$stopWatchId = $gdJob['command'] . 'DataSet'.'-'.$gdJob['pid'];
				$stopWatch->start($stopWatchId);

				if ($this->preRelease) {
					$this->restApi->callsLog = array();

					$this->restApi->updateDataSet($gdJob['pid'], $definition);

					$e = $stopWatch->stop($stopWatchId);
					$this->eventsLog[$stopWatchId] = array(
						'duration' => $e->getDuration(),
						'time' => date('c'),
						'restApi' => $this->restApi->callsLog
					);
				} else {
					$clToolApi->debugLogUrl = null;
					$this->restApi->callsLog = array();
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
					$this->eventsLog[$stopWatchId] = array(
						'duration' => $e->getDuration(),
						'time' => date('c'),
						'clTool' => $clToolApi->output,
						'restApi' => $this->restApi->callsLog
					);
				}

			}



			// Upload to WebDav
			$webDav = new WebDav($bucketAttributes['gd']['username'], $bucketAttributes['gd']['password'], $webDavUrl);


			// Upload time dimensions
			$dimensionsToUpload = array();
			foreach ($createDateJobs as $gdJob) {
				if ($gdJob['includeTime']) {
					// Upload csv to WebDav only once for all projects
					if (in_array($gdJob['name'], $dimensionsToUpload)) {
						continue;
					}

					$stopWatchId = 'uploadTimeDimension-' . $gdJob['name'] . '-' . $gdJob['pid'];
					$stopWatch->start($stopWatchId);

					$dimensionName = Model::getId($gdJob['name']);
					$tmpFolderDimension = $this->tmpDir . '/' . $dimensionName;
					$tmpFolderNameDimension = $tmpFolderName . '-' . $dimensionName;

					mkdir($tmpFolderDimension);
					$timeDimensionManifest = $this->goodDataModel->getTimeDimensionDataLoadManifest($gdJob['name']);
					file_put_contents($tmpFolderDimension . '/upload_info.json', $timeDimensionManifest);
					copy($this->scriptsPath . '/time-dimension.csv', $tmpFolderDimension . '/data.csv');
					$webDav->prepareFolder($tmpFolderNameDimension);
					$webDav->upload($tmpFolderDimension . '/upload_info.json', $tmpFolderNameDimension);
					$webDav->upload($tmpFolderDimension . '/data.csv', $tmpFolderNameDimension);
					$dimensionsToUpload[] = $gdJob['name'];

					$e = $stopWatch->stop($stopWatchId);
					$this->eventsLog[$stopWatchId] = array('duration' => $e->getDuration(), 'time' => date('c'),
						'url' => $webDav->url, 'folder' => '/uploads/' . $tmpFolderNameDimension);
				}
			}

			if (!$webDavUrl) $webDavUrl = $webDav->url;

			// Upload dataSets
			foreach ($loadDataJobs as $gdJob) {
				$stopWatchId = 'transferCsv-' . $gdJob['pid'];
				$stopWatch->start($stopWatchId);
				$webDavFolder = $tmpFolderName . '-' . $gdJob['pid'];

				$webDav->prepareFolder($webDavFolder);

				$this->csvHandler->initDownload($params['tableId'], $job['token'], $this->mainConfig['storage_api.url'],
					$this->mainConfig['user_agent'], $gdJob['incrementalLoad'], $gdJob['filterColumn'], $gdJob['pid']);
				$this->csvHandler->prepareTransformation($definition);
				$this->csvHandler->runUpload($bucketAttributes['gd']['username'], $bucketAttributes['gd']['password'], $webDavUrl, '/uploads/' . $webDavFolder);
				if (!$webDav->fileExists($webDavFolder . '/data.csv')) {
					throw new WebDavException(sprintf("Csv file has not been uploaded to '%s/uploads/%s/data.csv'", $webDavUrl, $webDavFolder));
				}

				$e = $stopWatch->stop($stopWatchId);
				$this->eventsLog[$stopWatchId] = array('duration' => $e->getDuration(), 'time' => date('c'));

				$stopWatchId = 'uploadManifest-' . $gdJob['pid'];
				$stopWatch->start($stopWatchId);

				$webDav->upload($this->tmpDir . '/upload_info.json', $webDavFolder);
				$e = $stopWatch->stop($stopWatchId);
				$this->eventsLog[$stopWatchId] = array('duration' => $e->getDuration(), 'time' => date('c'),
					'url' => $webDavUrl, 'folder' => '/uploads/' . $webDavFolder);
			}



			// Run ETL tasks
			$gdWriteStartTime = date('c');


			// Run ETL task of time dimensions
			foreach ($createDateJobs as $gdJob) {
				if ($gdJob['includeTime']) {
					$stopWatchId = 'runEtlTimeDimension-' . $gdJob['name'];
					$stopWatch->start($stopWatchId);

					$dimensionName = Model::getId($gdJob['name']);
					$dataSetName = 'time.' . $dimensionName;
					$tmpFolderDimension = $this->tmpDir . '/' . $dimensionName;
					$tmpFolderNameDimension = $tmpFolderName . '-' . $dimensionName;

					try {
						$this->restApi->loadData($gdJob['pid'], $tmpFolderNameDimension, $dataSetName);
					} catch (RestApiException $e) {
						$debugFile = $tmpFolderDimension . '/' . $gdJob['pid'] . '-etl.log';
						$taskName = 'Dimension ' . $gdJob['name'];
						$webDav->saveLogs($tmpFolderDimension, $debugFile);
						$logUrl = $this->s3Client->uploadFile($debugFile, 'text/plain', sprintf('%s/%s/%s-etl.log', $tmpFolderName, $gdJob['pid'], $dataSetName));
						if ($gdJob['mainProject']) {
							$debug[$taskName] = $logUrl;
						} else {
							$debug[$gdJob['pid']][$taskName] = $logUrl;
						}

						throw new RestApiException('ETL load failed', $e->getMessage());
					}

					$e = $stopWatch->stop($stopWatchId);
					$this->eventsLog[$stopWatchId] = array(
						'duration' => $e->getDuration(),
						'time' => date('c'),
						'restApi' => $this->restApi->callsLog
					);
				}
			}


			// Run ETL task of dataSets
			foreach ($loadDataJobs as $gdJob) {
				$stopWatchId = 'runEtl-' . $gdJob['pid'];
				$stopWatch->start($stopWatchId);
				$this->restApi->callsLog = array();
				$dataTmpDir = $this->tmpDir . '/' . $gdJob['pid'];
				if (!file_exists($dataTmpDir)) mkdir($dataTmpDir);
				$webDavFolder = $tmpFolderName . '-' . $gdJob['pid'];

				try {
					$this->restApi->loadData($gdJob['pid'], $webDavFolder, Model::getId($dataSetName));
				} catch (RestApiException $e) {
					$debugFile = $dataTmpDir . '/etl.log';
					$taskName = 'Data load';
					$webDav->saveLogs($webDavFolder, $debugFile);
					$logUrl = $this->s3Client->uploadFile($debugFile, 'text/plain', sprintf('%s/%s/%s-etl.log', $tmpFolderName, $gdJob['pid'], $dataSetName));
					if ($gdJob['mainProject']) {
						$debug[$taskName] = $logUrl;
					} else {
						$debug[$gdJob['pid']][$taskName] = $logUrl;
					}

					throw new RestApiException('ETL load failed', $e->getMessage());
				}
				$e = $stopWatch->stop($stopWatchId);
				$this->eventsLog[$stopWatchId] = array(
					'duration' => $e->getDuration(),
					'time' => date('c'),
					'restApi' => $this->restApi->callsLog
				);
			}
		} catch (\Exception $e) {
			$error = $e->getMessage();
			$event = $stopWatch->stop($stopWatchId);
			$this->eventsLog[$stopWatchId] = array(
				'duration' => $event->getDuration(),
				'time' => date('c')
			);

			if ($e instanceof CLToolApiErrorException) {
				if ($clToolApi->debugLogUrl) {
					$debug[(count($debug) + 1) . ': CL tool'] = $clToolApi->debugLogUrl;
					$clToolApi->debugLogUrl = null;
				}
				$this->eventsLog[$stopWatchId]['clTool'] = $clToolApi->output;
				$data = $e->getData();
				if (count($data)) {
					$debug[(count($debug) + 1) . ': debug data'] = $this->s3Client->uploadString($job['id'] . '/debug-data.json', json_encode($data));
				}
			} elseif ($e instanceof RestApiException) {
				$error = $e->getDetails();
				$this->eventsLog[$stopWatchId]['restApi'] = $this->restApi->callsLog;
			} elseif ($e instanceof WebDavException) {
				// Do nothing
			} else {
				throw $e;
			}
		}

		$result = array(
			'status' => !empty($error) ? 'error' : 'success',
			'debug' => json_encode($debug),
			'incrementalLoad' => (int) $incrementalLoad
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

	private function getWebDavUrl($bucketAttributes)
	{
		$webDavUrl = null;
		if (isset($bucketAttributes['gd']['backendUrl']) && $bucketAttributes['gd']['backendUrl'] != RestApi::DEFAULT_BACKEND_URL) {

			// Get WebDav url for non-default backend
			$backendUrl = (substr($bucketAttributes['gd']['backendUrl'], 0, 8) != 'https://'
					? 'https://' : '') . $bucketAttributes['gd']['backendUrl'];
			$this->restApi->setBaseUrl($backendUrl);
			$this->restApi->login($this->mainConfig['gd']['username'], $this->mainConfig['gd']['password']);
			$webDavUrl = $this->restApi->getWebDavUrl();
			if (!$webDavUrl) {
				throw new JobProcessException(sprintf("Getting of WebDav url for backend '%s' failed.", $bucketAttributes['gd']['backendUrl']));
			}
		}
		return $webDavUrl;
	}

}
