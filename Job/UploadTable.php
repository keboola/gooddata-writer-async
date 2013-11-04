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
use Symfony\Component\DependencyInjection\Tests\DefinitionDecoratorTest;

class UploadTable extends AbstractJob
{
	/**
	 * @var CsvHandler
	 */
	private $_csvHandler;
	
	public function run($job, $params)
	{
		if (empty($job['xmlFile'])) {
			throw new WrongConfigurationException("Parameter 'xmlFile' is missing");
		}
		$this->configuration->checkGoodDataSetup();

		// Init
		$startTime = time();
		$tmpFolderName = basename($this->tmpDir);
		$this->_csvHandler = new CsvHandler($this->scriptsPath, $this->s3Client, $this->tmpDir, $job['id']);
		$projects = $this->configuration->getProjects();
		$csvFileSize = 0;
		$output = null;
		$error = false;
		$debug = array();

		$createDateJobs = array();
		$updateModelJobs = array();
		$loadDataJobs = array();

		$tableDefinition = $this->configuration->getTableDefinition($params['tableId']);
		$incrementalLoad = (isset($params['incrementalLoad'])) ? $params['incrementalLoad']
			: (!empty($tableDefinition['incrementalLoad']) ? $tableDefinition['incrementalLoad'] : 0);
		$sanitize = (isset($params['sanitize'])) ? $params['sanitize'] : !empty($tableDefinition['sanitize']);
		$filterColumn = $this->_getFilterColumn($params['tableId'], $tableDefinition);
		$zipPath = isset($this->mainConfig['zip_path']) ? $this->mainConfig['zip_path'] : null;
		$webDavUrl = $this->_getWebDavUrl();

		$this->restApi->setCredentials($this->configuration->bucketInfo['gd']['username'], $this->configuration->bucketInfo['gd']['password']);



		// Get xml
		$xmlFile = $job['xmlFile'];
		try {
			if (!is_file($xmlFile)) {
				$xmlFile = $this->_csvHandler->downloadXml($xmlFile);
			}
			$xmlFileObject = $this->_csvHandler->getXml($xmlFile);
		} catch (CsvHandlerException $e) {
			$this->log->warn('Download of dataset xml failed', array(
				'exception' => $e->getMessage(),
				'trace' => $e->getTraceAsString(),
				'job' => $job,
				'params' => $params
			));
			throw new JobProcessException('Download of dataset xml failed');
		}

		$dataSetName = RestApi::gdName($xmlFileObject->name);
		$dataSetId = RestApi::datasetId($xmlFileObject->name);


		// Get manifest
		$manifest = $this->_csvHandler->getManifest($xmlFileObject, $incrementalLoad);
		file_put_contents($this->tmpDir . '/upload_info.json', json_encode($manifest));
		$manifestUrl = $this->s3Client->uploadFile($this->tmpDir . '/upload_info.json', 'text/plain', $tmpFolderName . '/manifest.json');


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
			}
		}


		// Enqueue jobs for creation/update of dataset and load data
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

		$clToolApi = new CLToolApi($this->log);
		$clToolApi->s3client = $this->s3Client;
		if (isset($this->configuration->bucketInfo['gd']['backendUrl'])) {
			$clToolApi->setBackendUrl($this->configuration->bucketInfo['gd']['backendUrl']);
		}
		$clToolApi->setCredentials($this->configuration->bucketInfo['gd']['username'], $this->configuration->bucketInfo['gd']['password']);

		// Start GoodData transfer
		$gdWriteStartTime = date('c');
		try {
			$webDav = new WebDav($this->configuration->bucketInfo['gd']['username'], $this->configuration->bucketInfo['gd']['password'], $webDavUrl, $zipPath);

			$uploadedDimensions = array();
			foreach ($createDateJobs as $gdJob) {
				$this->restApi->createDateDimension($gdJob['pid'], $gdJob['name'], $gdJob['includeTime']);
				if ($gdJob['includeTime']) {
					$dimensionName = RestApi::gdName($gdJob['name']);
					$tmpFolderDimension = $this->tmpDir . '/' . $dimensionName;
					$tmpFolderNameDimension = $tmpFolderName . '-' . $dimensionName;

					// Upload csv to WebDav only once for all projects
					if (!in_array($gdJob['name'], $uploadedDimensions)) {
						mkdir($tmpFolderDimension);
						$timeDimensionManifest = $this->_csvHandler->getTimeDimensionManifest($gdJob['name']);
						file_put_contents($tmpFolderDimension . '/upload_info.json', $timeDimensionManifest);
						copy($this->scriptsPath . '/time-dimension.csv', $tmpFolderDimension . '/data.csv');
						$csvFileSize += filesize($tmpFolderDimension . '/data.csv');
						$webDav->upload($tmpFolderDimension, $tmpFolderNameDimension, $tmpFolderDimension . '/upload_info.json', $tmpFolderDimension . '/data.csv');
						$uploadedDimensions[] = $gdJob['name'];
					}

					$result = $this->restApi->loadData($gdJob['pid'], $tmpFolderNameDimension);
					$debugFile = $tmpFolderDimension . '/' . $gdJob['pid'] . '-etl.log';
					$this->_checkEtlError($result, $webDav, $gdJob, 'time.' . $dimensionName, $debugFile,
						$tmpFolderDimension, $tmpFolderName, 'Dimension ' . $gdJob['name']);
				}
			}

			foreach ($updateModelJobs as $gdJob) {
				$clToolApi->debugLogUrl = null;
				$clToolApi->s3Dir = $tmpFolderName . '/' . $gdJob['pid'];
				$clToolApi->tmpDir = $this->tmpDir . '/' . $gdJob['pid'];
				if (!file_exists($clToolApi->tmpDir)) mkdir($clToolApi->tmpDir);
				if ($gdJob['command'] == 'create') {
					$clToolApi->createDataset($gdJob['pid'], $xmlFile);
				} else {
					$clToolApi->updateDataset($gdJob['pid'], $xmlFile, 1);
				}
				if ($clToolApi->debugLogUrl) {
					if ($gdJob['mainProject']) {
						$debug[$gdJob['command'] . ' dataset'] = $clToolApi->debugLogUrl;
					} else {
						$debug[$gdJob['pid']][$gdJob['command'] . ' dataset'] = $clToolApi->debugLogUrl;
					}
					$clToolApi->debugLogUrl = null;
				}
				$output .= $clToolApi->output;
			}

			$debug['manifest'] = $manifestUrl;
			foreach ($loadDataJobs as $gdJob) {
				$dataTmpDir = $this->tmpDir . '/' . $gdJob['pid'];
				if (!file_exists($dataTmpDir)) mkdir($dataTmpDir);

				$this->_csvHandler->initDownload($params['tableId'], $job['token'], $this->mainConfig['storageApi.url'],
					$this->mainConfig['user_agent'], $gdJob['incrementalLoad'], $gdJob['filterColumn'], $gdJob['pid']);
				if ($sanitize) {
					$this->_csvHandler->prepareSanitization($xmlFileObject);
				}
				$this->_csvHandler->prepareTransformation($xmlFileObject);
				$this->_csvHandler->runDownload($dataTmpDir . '/data.csv');
				$csvFileSize += filesize($dataTmpDir . '/data.csv');

				$webDavFolder = $tmpFolderName . '-' . $gdJob['pid'];
				$webDav->upload($dataTmpDir, $webDavFolder, $this->tmpDir . '/upload_info.json', $dataTmpDir . '/data.csv');

				// Run load task
				try {
					$result = $this->restApi->loadData($gdJob['pid'], $webDavFolder);
					$this->_checkEtlError($result, $webDav, $gdJob, $dataSetName, $dataTmpDir . '/etl.log', $webDavFolder, $tmpFolderName, 'Data Load');
				} catch (RestApiException $e) {
					throw new RestApiException('ETL load failed: ' . $e->getMessage());
				}
			}
		} catch (CLToolApiErrorException $e) {
			if ($clToolApi->debugLogUrl) {
				$debug[(count($debug) + 1) . ': CL tool'] = $clToolApi->debugLogUrl;
				$clToolApi->debugLogUrl = null;
			}
			$error = $e->getMessage();
		} catch (RestApiException $e) {
			$error = $e->getMessage();
		} catch (UnauthorizedException $e) {
			$error = 'Bad GoodData Credentials: ' . $e->getMessage();
		}

		$callsLog = $this->restApi->callsLog();
		if ($callsLog) {
			$output .= "\n\nRest API:\n" . $callsLog;
		}

		if (empty($tableDefinition['isExported'])) {
			$this->configuration->setTableAttribute($params['tableId'], 'export', 1);
		}
		$this->configuration->setTableAttribute($params['tableId'], 'lastExportDate', date('c', $startTime));
		$result = array(
			'status' => $error ? 'error' : 'success',
			'debug' => json_encode($debug),
			'gdWriteBytes' => $csvFileSize,
			'gdWriteStartTime' => $gdWriteStartTime
		);
		if ($error) {
			$result['error'] = $error;
		}
		return $this->_prepareResult($job['id'], $result, $output, $tmpFolderName);
	}


	private function _getFilterColumn($tableId, $tableDefinition)
	{
		$filterColumn = null;
		if (isset($this->configuration->bucketInfo['filterColumn']) && empty($tableDefinition['ignoreFilter'])) {
			$filterColumn = $this->configuration->bucketInfo['filterColumn'];
			$tableInfo = $this->configuration->getTable($tableId);
			if (!in_array($filterColumn, $tableInfo['columns'])) {
				throw new WrongConfigurationException("Filter column does not exist in the table");
			}
			if (!in_array($filterColumn, $tableInfo['indexedColumns'])) {
				throw new WrongConfigurationException("Filter column does not have index");
			}
		}
		return $filterColumn;
	}

	private function _getWebDavUrl()
	{
		$webDavUrl = null;
		if (isset($this->configuration->bucketInfo['gd']['backendUrl']) && $this->configuration->bucketInfo['gd']['backendUrl'] != RestApi::DEFAULT_BACKEND_URL) {

			//@TODO Temporal log
			$this->log->alert('Non-default backend', array(
				'projectId' => $this->configuration->projectId,
				'writerId' => $this->configuration->writerId,
				'backendUrl' => $this->configuration->bucketInfo['gd']['backendUrl']
			));

			// Get WebDav url for non-default backend
			$backendUrl = (substr($this->configuration->bucketInfo['gd']['backendUrl'], 0, 8) != 'https://'
					? 'https://' : '') . $this->configuration->bucketInfo['gd']['backendUrl'];
			$this->restApi->setBaseUrl($backendUrl);
			$this->restApi->setCredentials($this->mainConfig['gd']['username'], $this->mainConfig['gd']['password']);
			$gdc = $this->restApi->get('/gdc');
			if (isset($gdc['about']['links'])) foreach ($gdc['about']['links'] as $link) {
				if ($link['category'] == 'uploads') {
					$webDavUrl = $link['link'];
					break;
				}
			}
			if (!$webDavUrl) {
				throw new JobProcessException(sprintf("Getting of WebDav url for backend '%s' failed.", $this->configuration->bucketInfo['gd']['backendUrl']));
			}
		}
		return $webDavUrl;
	}

	/**
	 * @param $result
	 * @param WebDav $webDav
	 * @param $gdJob
	 * @param $dataSetName
	 * @param $debugFile
	 * @param $tmpFolder
	 * @param $tmpFolderName
	 * @param $taskName
	 * @throws \Keboola\GoodDataWriter\GoodData\RestApiException
	 */
	private function _checkEtlError($result, $webDav, $gdJob, $dataSetName, $debugFile, $tmpFolder, $tmpFolderName, $taskName)
	{
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
	}

}
