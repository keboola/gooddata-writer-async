<?php
/**
 * @author Jakub Matejka <jakub@keboola.com>
 * @date 2013-04-17
 */

namespace Keboola\GoodDataWriter\Job;

use Keboola\GoodDataWriter\Exception\WrongConfigurationException,
	Keboola\GoodDataWriter\Exception\JobProcessException,
	Keboola\GoodDataWriter\GoodData\CLToolApiErrorException,
	Keboola\GoodDataWriter\GoodData\RestApiException,
	Keboola\GoodDataWriter\GoodData\UnauthorizedException;
use Keboola\GoodDataWriter\GoodData\CLToolApi,
	Keboola\GoodDataWriter\GoodData\RestApi,
	Keboola\GoodDataWriter\GoodData\CsvHandler,
	Keboola\GoodDataWriter\GoodData\CsvHandlerException,
	Keboola\GoodDataWriter\GoodData\WebDav;
use Symfony\Component\Stopwatch\Stopwatch;

class UploadTable extends AbstractJob
{
	/**
	 * @var CsvHandler
	 */
	private $_csvHandler;
	
	public function run($job, $params)
	{
		$eventsLog = array();
		$eventsLog['start'] = array('duration' => 0, 'time' => date('c'));
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
		$this->_csvHandler = new CsvHandler($this->scriptsPath, $this->s3Client, $this->tmpDir, $job['id']);
		$projects = $this->configuration->getProjects();
		$error = false;
		$debug = array();

		$createDateJobs = array();
		$updateModelJobs = array();
		$loadDataJobs = array();

		$tableDefinition = $this->configuration->getDataSet($params['tableId']);
		$incrementalLoad = (isset($params['incrementalLoad'])) ? $params['incrementalLoad']
			: (!empty($tableDefinition['incrementalLoad']) ? $tableDefinition['incrementalLoad'] : 0);
		$filterColumn = $this->_getFilterColumn($params['tableId'], $tableDefinition, $bucketAttributes);

		$webDavUrl = $this->_getWebDavUrl($bucketAttributes);
		$this->restApi->login($bucketAttributes['gd']['username'], $bucketAttributes['gd']['password']);

		$e = $stopWatch->stop($stopWatchId);
		$eventsLog[$stopWatchId] = array('duration' => $e->getDuration(), 'time' => date('c'));

		// Get xml
		$stopWatchId = 'getXml';
		$stopWatch->start($stopWatchId);
		$xmlFile = $job['xmlFile'];
		try {
			if (!is_file($xmlFile)) {
				$xmlFile = $this->_csvHandler->downloadXml($xmlFile);
			}
			$xmlFileObject = $this->_csvHandler->getXml($xmlFile);
		} catch (CsvHandlerException $e) {
			$this->log->warn('Download of data set xml failed', array(
				'exception' => $e->getMessage(),
				'trace' => $e->getTraceAsString(),
				'job' => $job,
				'params' => $params
			));
			throw new JobProcessException('Download of data set xml failed');
		}
		$e = $stopWatch->stop($stopWatchId);
		$eventsLog[$stopWatchId] = array('duration' => $e->getDuration(), 'time' => date('c'));

		$dataSetName = RestApi::gdName($xmlFileObject->name);
		$dataSetId = RestApi::datasetId($xmlFileObject->name);


		// Get manifest
		$stopWatchId = 'getManifest';
		$stopWatch->start($stopWatchId);
		$manifest = $this->_csvHandler->getManifest($xmlFileObject, $incrementalLoad);
		file_put_contents($this->tmpDir . '/upload_info.json', json_encode($manifest));
		$debug['manifest'] = $this->s3Client->uploadFile($this->tmpDir . '/upload_info.json', 'text/plain', $tmpFolderName . '/manifest.json');
		$e = $stopWatch->stop($stopWatchId);
		$eventsLog[$stopWatchId] = array('duration' => $e->getDuration(), 'time' => date('c'));


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
		if ($xmlFileObject->columns) foreach ($xmlFileObject->columns->column as $column) if ((string)$column->ldmType == 'DATE') {
			if (!$dateDimensions) {
				$dateDimensions = $this->configuration->getDateDimensions();
			}

			$dimension = (string)$column->schemaReference;
			if (!isset($dateDimensions[$dimension])) {
				throw new WrongConfigurationException("Date dimension '$dimension' does not exist");
			}

			if (!in_array($dimension, array_keys($dateDimensionsToLoad))) {
				$dateDimensionsToLoad[$dimension] = array(
					'name' => $dimension,
					'gdName' => RestApi::gdName($dimension),
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
				if (!in_array(RestApi::dimensionId($dimension['gdName']), array_keys($project['existingDataSets']))) {
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
				'incrementalLoad' => $incrementalLoad && $dataSetExists
			);
		}



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
		$e = $stopWatch->stop($stopWatchId);
		$eventsLog[$stopWatchId] = array('duration' => $e->getDuration(), 'time' => date('c'));



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
				$eventsLog[$stopWatchId] = array(
					'duration' => $e->getDuration(),
					'time' => date('c'),
					'restApi' => $this->restApi->callsLog
				);
			}


			// Update model
			foreach ($updateModelJobs as $gdJob) {
				$stopWatchId = $gdJob['command'] . 'DataSet'.'-'.$gdJob['pid'];
				$stopWatch->start($stopWatchId);
				$clToolApi->debugLogUrl = null;
				$this->restApi->callsLog = array();
				$clToolApi->s3Dir = $tmpFolderName . '/' . $gdJob['pid'];
				$clToolApi->tmpDir = $this->tmpDir . '/' . $gdJob['pid'];
				if (!file_exists($clToolApi->tmpDir)) mkdir($clToolApi->tmpDir);
				if ($gdJob['command'] == 'create') {
					$maql = $clToolApi->createDataSetMaql($gdJob['pid'], $xmlFile);
					$this->restApi->executeMaql($gdJob['pid'], $maql);

					if (empty($tableDefinition['isExported'])) {
						// Save export status to definition
						$this->configuration->updateDataSetDefinition($params['tableId'], 'isExported', 1);
					}
				} else {
					$maql = $clToolApi->updateDataSetMaql($gdJob['pid'], $xmlFile, 1);
					$this->restApi->executeMaql($gdJob['pid'], $maql);
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
				$eventsLog[$stopWatchId] = array(
					'duration' => $e->getDuration(),
					'time' => date('c'),
					'clTool' => $clToolApi->output,
					'restApi' => $this->restApi->callsLog
				);
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

					$dimensionName = RestApi::gdName($gdJob['name']);
					$tmpFolderDimension = $this->tmpDir . '/' . $dimensionName;
					$tmpFolderNameDimension = $tmpFolderName . '-' . $dimensionName;

					mkdir($tmpFolderDimension);
					$timeDimensionManifest = $this->_csvHandler->getTimeDimensionManifest($gdJob['name']);
					file_put_contents($tmpFolderDimension . '/upload_info.json', $timeDimensionManifest);
					copy($this->scriptsPath . '/time-dimension.csv', $tmpFolderDimension . '/data.csv');
					$webDav->prepareFolder($tmpFolderNameDimension);
					$webDav->upload($tmpFolderDimension . '/upload_info.json', $tmpFolderNameDimension);
					$webDav->upload($tmpFolderDimension . '/data.csv', $tmpFolderNameDimension);
					$dimensionsToUpload[] = $gdJob['name'];

					$e = $stopWatch->stop($stopWatchId);
					$eventsLog[$stopWatchId] = array('duration' => $e->getDuration(), 'time' => date('c'),
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

				$this->_csvHandler->initDownload($params['tableId'], $job['token'], $this->mainConfig['storage_api.url'],
					$this->mainConfig['user_agent'], $gdJob['incrementalLoad'], $gdJob['filterColumn'], $gdJob['pid']);
				$this->_csvHandler->prepareTransformation($xmlFileObject);
				$this->_csvHandler->runUpload($bucketAttributes['gd']['username'], $bucketAttributes['gd']['password'], $webDavUrl, '/uploads/' . $webDavFolder);

				$e = $stopWatch->stop($stopWatchId);
				$eventsLog[$stopWatchId] = array('duration' => $e->getDuration(), 'time' => date('c'));

				$stopWatchId = 'uploadManifest-' . $gdJob['pid'];
				$stopWatch->start($stopWatchId);

				$webDav->upload($this->tmpDir . '/upload_info.json', $webDavFolder);
				$e = $stopWatch->stop($stopWatchId);
				$eventsLog[$stopWatchId] = array('duration' => $e->getDuration(), 'time' => date('c'),
					'url' => $webDavUrl, 'folder' => '/uploads/' . $webDavFolder);
			}



			// Run ETL tasks
			$gdWriteStartTime = date('c');


			// Run ETL task of time dimensions
			foreach ($createDateJobs as $gdJob) {
				if ($gdJob['includeTime']) {
					$stopWatchId = 'runEtlTimeDimension-' . $gdJob['name'];
					$stopWatch->start($stopWatchId);

					$dimensionName = RestApi::gdName($gdJob['name']);
					$tmpFolderDimension = $this->tmpDir . '/' . $dimensionName;
					$tmpFolderNameDimension = $tmpFolderName . '-' . $dimensionName;

					$result = $this->restApi->loadData($gdJob['pid'], $tmpFolderNameDimension);
					$debugFile = $tmpFolderDimension . '/' . $gdJob['pid'] . '-etl.log';

					// Get upload error
					$dataSetName = 'time.' . $dimensionName;
					$tmpFolder = $tmpFolderDimension;
					$taskName = 'Dimension ' . $gdJob['name'];
					if ($result['taskStatus'] == 'ERROR' || $result['taskStatus'] == 'WARNING') {
						// Find upload message
						$uploadMessage = $this->restApi->getUploadMessage($gdJob['pid'], $dataSetName);
						if ($uploadMessage) {
							file_put_contents($debugFile, $uploadMessage . PHP_EOL . PHP_EOL, FILE_APPEND);
						}

						// Look for .json and .log files in WebDav folder
						$webDav->saveLogs($tmpFolder, $debugFile);
						$logUrl = $this->s3Client->uploadFile($debugFile, 'text/plain', sprintf('%s/%s/%s-etl.log', $tmpFolderName, $gdJob['pid'], $dataSetName));
						if ($gdJob['mainProject']) {
							$debug[$taskName] = $logUrl;
						} else {
							$debug[$gdJob['pid']][$taskName] = $logUrl;
						}

						throw new RestApiException($taskName . ' Error. ' . $uploadMessage);
					}

					$e = $stopWatch->stop($stopWatchId);
					$eventsLog[$stopWatchId] = array(
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
					$result = $this->restApi->loadData($gdJob['pid'], $webDavFolder);

					// Get upload error
					$debugFile = $dataTmpDir . '/etl.log';
					$tmpFolder = $webDavFolder;
					$taskName = 'Data load';
					if ($result['taskStatus'] == 'ERROR' || $result['taskStatus'] == 'WARNING') {
						// Find upload message
						$uploadMessage = $this->restApi->getUploadMessage($gdJob['pid'], $dataSetName);
						if ($uploadMessage) {
							file_put_contents($debugFile, $uploadMessage . PHP_EOL . PHP_EOL, FILE_APPEND);
						}

						// Look for .json and .log files in WebDav folder
						$webDav->saveLogs($tmpFolder, $debugFile);
						$logUrl = $this->s3Client->uploadFile($debugFile, 'text/plain', sprintf('%s/%s/%s-etl.log', $tmpFolderName, $gdJob['pid'], $dataSetName));
						if ($gdJob['mainProject']) {
							$debug[$taskName] = $logUrl;
						} else {
							$debug[$gdJob['pid']][$taskName] = $logUrl;
						}

						throw new RestApiException($taskName . ' Error. ' . $uploadMessage);
					}
				} catch (RestApiException $e) {
					throw new RestApiException('ETL load failed: ' . $e->getMessage());
				}
				$e = $stopWatch->stop($stopWatchId);
				$eventsLog[$stopWatchId] = array(
					'duration' => $e->getDuration(),
					'time' => date('c'),
					'restApi' => $this->restApi->callsLog
				);
			}
		} catch (CLToolApiErrorException $e) {
			if ($clToolApi->debugLogUrl) {
				$debug[(count($debug) + 1) . ': CL tool'] = $clToolApi->debugLogUrl;
				$clToolApi->debugLogUrl = null;
			}
			$error = $e->getMessage();
			$event = $stopWatch->stop($stopWatchId);
			$eventsLog[$stopWatchId] = array(
				'duration' => $event->getDuration(),
				'time' => date('c'),
				'clTool' => $clToolApi->output
			);
		} catch (RestApiException $e) {
			$error = $e->getMessage();
			$event = $stopWatch->stop($stopWatchId);
			$eventsLog[$stopWatchId] = array(
				'duration' => $event->getDuration(),
				'time' => date('c'),
				'restApi' => $this->restApi->callsLog
			);
		} catch (UnauthorizedException $e) {
			$error = 'Bad GoodData Credentials: ' . $e->getMessage();
			$event = $stopWatch->stop($stopWatchId);
			$eventsLog[$stopWatchId] = array(
				'duration' => $event->getDuration(),
				'time' => date('c'),
				'restApi' => $this->restApi->callsLog,
				'clTool' => $clToolApi->output
			);
		}

		$result = array(
			'status' => $error ? 'error' : 'success',
			'debug' => json_encode($debug)
		);
		if (!empty($gdWriteStartTime)) {
			$result['gdWriteStartTime'] = $gdWriteStartTime;
		}
		if ($error) {
			$result['error'] = $error;
		}

		return $this->_prepareResult($job['id'], $result, $eventsLog, $tmpFolderName);
	}


	private function _getFilterColumn($tableId, $tableDefinition, $bucketAttributes)
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

	private function _getWebDavUrl($bucketAttributes)
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
