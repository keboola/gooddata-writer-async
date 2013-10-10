<?php
/**
 * @author Jakub Matejka <jakub@keboola.com>
 * @date 2013-04-17
 */

namespace Keboola\GoodDataWriter\Job;

use Keboola\GoodDataWriter\Exception\ClientException,
	Keboola\GoodDataWriter\Exception\WrongConfigurationException,
	Keboola\GoodDataWriter\Exception\JobProcessException,
	Keboola\GoodDataWriter\GoodData\CLToolApiErrorException,
	Keboola\GoodDataWriter\GoodData\RestApiException,
	Keboola\GoodDataWriter\GoodData\UnauthorizedException;
use Keboola\GoodDataWriter\GoodData\CLToolApi,
	Keboola\GoodDataWriter\GoodData\CsvHandler,
	Keboola\GoodDataWriter\GoodData\CsvHandlerException;
use Keboola\GoodDataWriter\GoodData\WebDav,
	Keboola\GoodDataWriter\GoodData\WebDavException;
use Keboola\StorageApi\Client as StorageApiClient;

class UploadTable extends AbstractJob
{
	public function run($job, $params)
	{
		if (empty($job['xmlFile'])) {
			throw new WrongConfigurationException("Parameter 'xmlFile' is missing");
		}
		$this->configuration->checkGoodDataSetup();

		// Init
		$startTime = time();
		$tmpFolderName = basename($this->tmpDir);
		$csvHandler = new CsvHandler($this->scriptsPath, $this->s3Client, $this->tmpDir);
		$projects = $this->configuration->getProjects();
		$csvFileSize = 0;
		$output = null;
		$error = false;
		$debug = array();

		$createDateJobs = array();
		$updateModelJobs = array();
		$loadDataJobs = array();

		if (isset($params['pid'])) {
			$chosenProject = null;
			foreach ($projects as $project) {
				if ($project['pid'] == $params['pid']) {
					$chosenProject = $project;
					break;
				}
			}
			if (!$chosenProject) {
				throw new WrongConfigurationException("Project '" . $params['pid'] . "' was not found in configuration");
			}
			$projects = array($chosenProject);
		}

		$tableDefinition = $this->configuration->getTableDefinition($params['tableId']);
		$incrementalLoad = (isset($params['incrementalLoad'])) ? $params['incrementalLoad']
			: (!empty($tableDefinition['incrementalLoad']) ? $tableDefinition['incrementalLoad'] : 0);
		if (empty($tableDefinition['lastExportDate'])) {
			$incrementalLoad = 0;
		}
		$sanitize = (isset($params['sanitize'])) ? $params['sanitize'] : !empty($tableDefinition['sanitize']);

		$filterColumn = null;
		if (isset($this->configuration->bucketInfo['filterColumn']) && empty($tableDefinition['ignoreFilter'])) {
			$filterColumn = $this->configuration->bucketInfo['filterColumn'];
			$tableInfo = $this->configuration->getTable($params['tableId']);
			if (!in_array($filterColumn, $tableInfo['columns'])) {
				throw new WrongConfigurationException("Filter column does not exist in the table");
			}
			if (!in_array($filterColumn, $tableInfo['indexedColumns'])) {
				throw new WrongConfigurationException("Filter column does not have index");
			}
		}

		$zipPath = isset($this->mainConfig['zip_path']) ? $this->mainConfig['zip_path'] : null;
		$webdavUrl = null;
		if (isset($this->configuration->bucketInfo['gd']['backendUrl'])) {
			// Get WebDav url for non-default backend
			$env = empty($params['dev']) ? 'prod' :'dev';
			$mainConfig = $this->mainConfig['gd'][$env];
			$this->restApi->setCredentials($mainConfig['username'], $mainConfig['password']);
			$gdc = $this->restApi->get('/gdc');
			if (isset($gdc['about']['links'])) foreach ($gdc['about']['links'] as $link) {
				if ($link['category'] == 'uploads') {
					$webdavUrl = $link['link'];
					break;
				}
			}
			if (!$webdavUrl) {
				throw new JobProcessException(sprintf("Getting of WebDav url for backend '%s' failed.", $this->configuration->bucketInfo['gd']['backendUrl']));
			}
		}

		$this->restApi->setCredentials($this->configuration->bucketInfo['gd']['username'], $this->configuration->bucketInfo['gd']['password']);



		// Get xml
		$xmlFile = $job['xmlFile'];
		try {
			if (!is_file($xmlFile)) {
				$xmlFile = $csvHandler->downloadXml($xmlFile);
			}
			$xmlFileObject = $csvHandler->getXml($xmlFile);
		} catch (CsvHandlerException $e) {
			$this->log->warn('Download of dataset xml failed', array(
				'exception' => $e->getMessage(),
				'trace' => $e->getTraceAsString(),
				'job' => $job,
				'params' => $params
			));
			throw new JobProcessException('Download of dataset xml failed');
		}

		$datasetName = CsvHandler::gdName($xmlFileObject->name);


		// Get manifest
		$manifest = $csvHandler->getManifest($xmlFileObject, $incrementalLoad);
		file_put_contents($this->tmpDir . '/upload_info.json', json_encode($manifest));
		$manifestUrl = $this->s3Client->uploadFile($this->tmpDir . '/upload_info.json', 'text/plain', $tmpFolderName . '/manifest.json');


		// Find out new date dimensions and enqueue them for creation
		$dateDimensions = null;
		if ($xmlFileObject->columns) foreach ($xmlFileObject->columns->column as $column) if ((string)$column->ldmType == 'DATE') {
			if (!$dateDimensions) {
				$dateDimensions = $this->configuration->getDateDimensions();
			}

			$dimension = (string)$column->schemaReference;
			if (!isset($dateDimensions[$dimension])) {
				throw new WrongConfigurationException("Date dimension '$dimension' does not exist");
			}

			if (empty($dateDimensions[$dimension]['lastExportDate'])) {
				foreach ($projects as $project) if ($project['active']) {
					$createDateJobs[] = array(
						'pid' => $project['pid'],
						'name' => $dateDimensions[$dimension]['name'],
						'includeTime' => !empty($dateDimensions[$dimension]['includeTime']),
						'mainProject' => !empty($project['main'])
					);
				}
			}
		}


		// Enqueue jobs for creation/update of dataset and load data
		$usedProjects = array();
		foreach ($projects as $project) if ($project['active']) {
			if (in_array($project['pid'], $usedProjects)) {
				throw new WrongConfigurationException("Project '" . $project['pid'] . "' is duplicated in configuration");
			}
			$usedProjects[] = $project['pid'];

			if (empty($tableDefinition['lastExportDate'])) {
				$updateModelJobs[] = array(
					'command' => 'create',
					'pid' => $project['pid'],
					'mainProject' => !empty($project['main'])
				);
			} else if (empty($tableDefinition['lastChangeDate']) || strtotime($tableDefinition['lastChangeDate']) > strtotime($tableDefinition['lastExportDate'])) {
				$updateModelJobs[] = array(
					'command' => 'update',
					'pid' => $project['pid'],
					'mainProject' => !empty($project['main'])
				);
			}

			$loadDataJobs[] = array(
				'command' => 'loadData',
				'pid' => $project['pid'],
				'filterColumn' => ($filterColumn && empty($project['main'])) ? $filterColumn : false,
				'mainProject' => !empty($project['main'])
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
			$webDav = new WebDav($this->configuration->bucketInfo['gd']['username'], $this->configuration->bucketInfo['gd']['password'], $webdavUrl, $zipPath);

			$uploadedDimensions = array();
			foreach ($createDateJobs as $gdJob) {
				$this->restApi->createDateDimension($gdJob['pid'], $gdJob['name'], $gdJob['includeTime']);
				if ($gdJob['includeTime']) {
					$dimensionName = CsvHandler::gdName($gdJob['name']);
					$tmpFolderDimension = $this->tmpDir . '/' . $dimensionName;
					$tmpFolderNameDimension = $tmpFolderName . '-' . $dimensionName;

					// Upload csv to WebDav only once for all projects
					if (!in_array($gdJob['name'], $uploadedDimensions)) {
						mkdir($tmpFolderDimension);
						$timeDimensionManifest = $csvHandler->getTimeDimensionManifest($gdJob['name']);
						file_put_contents($tmpFolderDimension . '/upload_info.json', $timeDimensionManifest);
						copy($this->scriptsPath . '/time-dimension.csv', $tmpFolderDimension . '/data.csv');
						$csvFileSize += filesize($tmpFolderDimension . '/data.csv');
						$webDav->upload($tmpFolderDimension, $tmpFolderNameDimension, $tmpFolderDimension . '/upload_info.json', $tmpFolderDimension . '/data.csv');
						$uploadedDimensions[] = $gdJob['name'];
					}

					$result = $this->restApi->loadData($gdJob['pid'], $tmpFolderNameDimension);
					if ($result['taskStatus'] == 'ERROR' || $result['taskStatus'] == 'WARNING') {
						$debugFile = $tmpFolderDimension . '/' . $gdJob['pid'] . '-etl.log';

						// Find upload message
						$uploadMessage = $this->restApi->getUploadMessage($gdJob['pid'], 'time.' . $dimensionName);
						if ($uploadMessage) {
							file_put_contents($debugFile, $uploadMessage . PHP_EOL . PHP_EOL, FILE_APPEND);
						}

						// Look for .json and .log files in WebDav folder
						$webDav->saveLogs($tmpFolderNameDimension, $debugFile);
						$logUrl = $this->s3Client->uploadFile($debugFile, 'text/plain', sprintf('%s/%s/%s-etl.log', $tmpFolderName, $gdJob['pid'], $dimensionName));
						if ($gdJob['mainProject']) {
							$debug['dimension ' . $gdJob['name']] = $logUrl;
						} else {
							$debug[$gdJob['pid']]['dimension ' . $gdJob['name']] = $logUrl;
						}

						throw new RestApiException('Create Dimension Error. ' . $uploadMessage);
					}
				}

				$this->configuration->setDateDimensionAttribute($gdJob['name'], 'lastExportDate', date('c', $startTime));
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

				$csvHandler->initDownload($params['tableId'], $job['token'], $this->mainConfig['storageApi.url'],
					$this->mainConfig['user_agent'], $incrementalLoad, $gdJob['filterColumn'], $gdJob['pid']);
				if ($sanitize) {
					$csvHandler->prepareSanitization($xmlFileObject);
				}
				$csvHandler->prepareTransformation($xmlFileObject);
				$csvHandler->runDownload($dataTmpDir . '/data.csv');
				$csvFileSize += filesize($dataTmpDir . '/data.csv');

				$webDavFolder = $tmpFolderName . '-' . $gdJob['pid'];
				$webDav->upload($dataTmpDir, $webDavFolder, $this->tmpDir . '/upload_info.json', $dataTmpDir . '/data.csv');

				// Run load task
				try {
					$result = $this->restApi->loadData($gdJob['pid'], $webDavFolder);
					if ($result['taskStatus'] == 'ERROR' || $result['taskStatus'] == 'WARNING') {
						$debugFile = $dataTmpDir . '/etl.log';

						// Find upload message
						$uploadMessage = $this->restApi->getUploadMessage($gdJob['pid'], $datasetName);
						if ($uploadMessage) {
							file_put_contents($debugFile, $uploadMessage . PHP_EOL . PHP_EOL, FILE_APPEND);
						}

						// Look for .json and .log files in WebDav folder
						$webDav->saveLogs($webDavFolder, $debugFile);
						$logUrl = $this->s3Client->uploadFile($debugFile, 'text/plain', sprintf('%s/%s/%s-etl.log', $tmpFolderName, $gdJob['pid'], $datasetName));
						if ($gdJob['mainProject']) {
							$debug['load data'] = $logUrl;
						} else {
							$debug[$gdJob['pid']]['load data'] = $logUrl;
						}

						throw new RestApiException('Load Data Error. ' . $uploadMessage);
					}
				} catch (RestApiException $e) {
					throw new RestApiException('ETL load failed: ' . $e->getMessage());
				}
			}
		} catch (CsvHandlerException $e) {
			$this->log->warn('Download of data csv failed', array(
				'exception' => $e->getMessage(),
				'trace' => $e->getTraceAsString(),
				'job' => $job,
				'params' => $params
			));
			throw new JobProcessException('Download of data csv failed');
		} catch (CLToolApiErrorException $e) {
			if ($clToolApi->debugLogUrl) {
				$debug[(count($debug) + 1) . ': CL tool'] = $clToolApi->debugLogUrl;
				$clToolApi->debugLogUrl = null;
			}
			$error = $e->getMessage();
		} catch (RestApiException $e) {
			$error = $e->getMessage();
		} catch (WebDavException $e) {
			$this->log->warn('Upload to GoodData WebDav failed', array(
				'exception' => $e->getMessage(),
				'trace' => $e->getTraceAsString(),
				'job' => $job,
				'params' => $params
			));
			$error = 'Upload to GoodData WebDav failed.';
		} catch (UnauthorizedException $e) {
			$error = 'Bad GoodData Credentials: ' . $e->getMessage();
		}

		$callsLog = $this->restApi->callsLog();
		if ($callsLog) {
			$output .= "\n\nRest API:\n" . $callsLog;
		}

		if (empty($tableDefinition['lastExportDate'])) {
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


}
