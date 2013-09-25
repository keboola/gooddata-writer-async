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
use Keboola\GoodDataWriter\GoodData\CsvHandler;
use Keboola\GoodDataWriter\GoodData\WebDav,
	Keboola\GoodDataWriter\GoodData\WebDavException;
use Keboola\StorageApi\Client as StorageApiClient;

class UploadTable extends GenericJob
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
		$csvHandler = new CsvHandler($this->rootPath, $this->s3Client, $this->tmpDir);
		$projects = $this->configuration->getProjects();
		$csvFileSize = 0;
		$output = null;
		$error = false;
		$debug = array();
		$gdJobs = array();

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
		if (!is_file($xmlFile)) {
			$xmlFile = $csvHandler->downloadXml($xmlFile);
		}
		$xmlFileObject = $csvHandler->getXml($xmlFile);

		$datasetName = CsvHandler::gdName($xmlFileObject->name);


		// Get manifest
		$manifest = $csvHandler->getManifest($xmlFileObject, $incrementalLoad);
		file_put_contents($this->tmpDir . '/upload_info.json', json_encode($manifest));
		$manifestUrl = $this->s3Client->uploadFile($this->tmpDir . '/upload_info.json', 'text/plain', $tmpFolderName . '/manifest.json');
		$debug['manifest'] = $manifestUrl;


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
					$gdJobs[] = array(
						'command' => 'createDate',
						'pid' => $project['pid'],
						'name' => $dateDimensions[$dimension]['name'],
						'includeTime' => !empty($dateDimensions[$dimension]['includeTime'])
					);
				}
			}
		}


		// Enqueue jobs for creation/update of dataset and load data
		foreach ($projects as $project) if ($project['active']) {
			if (empty($tableDefinition['lastExportDate'])) {
				$gdJobs[] = array(
					'command' => 'createDataset',
					'pid' => $project['pid']
				);
			} else if (empty($tableDefinition['lastChangeDate']) || strtotime($tableDefinition['lastChangeDate']) > strtotime($tableDefinition['lastExportDate'])) {
				$gdJobs[] = array(
					'command' => 'updateDataset',
					'pid' => $project['pid']
				);
			}

			$gdJobs[] = array(
				'command' => 'loadData',
				'pid' => $project['pid'],
				'filterColumn' => ($filterColumn && empty($project['main'])) ? $filterColumn : false
			);
		}


		// Start GoodData transfer
		$gdWriteStartTime = date('c');
		try {
			$webDav = new WebDav($this->configuration->bucketInfo['gd']['username'], $this->configuration->bucketInfo['gd']['password'], $webdavUrl, $zipPath);

			// Execute enqueued jobs
			foreach ($gdJobs as $gdJob) {
				$this->clToolApi->debugLogUrl = null;

				switch ($gdJob['command']) {
					case 'createDate':

						$dimensionName = CsvHandler::gdName($gdJob['name']);
						$tmpFolderDimension = $this->tmpDir . '/' . $dimensionName;
						mkdir($tmpFolderDimension);
						$tmpFolderNameDimension = $tmpFolderName . '-' . $dimensionName;
						$this->restApi->createDateDimension($gdJob['pid'], $gdJob['name'], $gdJob['includeTime']);
						if ($gdJob['includeTime']) {
							$timeDimensionManifest = $csvHandler->getTimeDimensionManifest($gdJob['name']);
							file_put_contents($tmpFolderDimension . '/upload_info.json', $timeDimensionManifest);
							copy($this->rootPath . '/GoodData/time-dimension.csv', $tmpFolderDimension . '/data.csv');
							$csvFileSize += filesize($tmpFolderDimension . '/data.csv');
							$webDav->upload($tmpFolderDimension, $tmpFolderNameDimension, 'upload_info.json', 'data.csv');

							$result = $this->restApi->loadData($gdJob['pid'], $tmpFolderNameDimension);
							if ($result['taskStatus'] == 'ERROR' || $result['taskStatus'] == 'WARNING') {
								$debugFile = $tmpFolderDimension . '/data-load-log.txt';

								// Find upload message
								$uploadMessage = $this->restApi->getUploadMessage($gdJob['pid'], 'time.' . $dimensionName);
								if ($uploadMessage) {
									file_put_contents($debugFile, $uploadMessage . PHP_EOL . PHP_EOL, FILE_APPEND);
								}

								// Look for .json and .log files in WebDav folder
								$webDav->saveLogs($tmpFolderNameDimension, $debugFile);
								$debug['timeDimension'] = $this->s3Client->uploadFile($debugFile);

								throw new RestApiException('Create Dimension Error. ' . $uploadMessage);
							}
						}

						$this->configuration->setDateDimensionAttribute($gdJob['name'], 'lastExportDate', date('c', $startTime));

						break;
					case 'createDataset':
						$this->clToolApi->setCredentials($this->configuration->bucketInfo['gd']['username'], $this->configuration->bucketInfo['gd']['password']);
						$this->clToolApi->createDataset($gdJob['pid'], $xmlFile);
						break;
					case 'updateDataset':
						$this->clToolApi->setCredentials($this->configuration->bucketInfo['gd']['username'], $this->configuration->bucketInfo['gd']['password']);
						$this->clToolApi->updateDataset($gdJob['pid'], $xmlFile, 1);

						break;
					case 'loadData':

						$csvHandler->initDownload($params['tableId'], $job['token'], $this->mainConfig['storageApi.url'],
							$this->mainConfig['user_agent'], $incrementalLoad, $gdJob['filterColumn'], $gdJob['pid']);
						if ($sanitize) {
							$csvHandler->prepareSanitization($xmlFileObject);
						}
						$csvHandler->prepareTransformation($xmlFileObject);
						$csvHandler->runDownload($this->tmpDir . '/data.csv');
						$csvFileSize += filesize($this->tmpDir . '/data.csv');

						$webDav->upload($this->tmpDir, $tmpFolderName, 'upload_info.json', 'data.csv');

						// Run load task
						try {
							$result = $this->restApi->loadData($gdJob['pid'], $tmpFolderName);
							if ($result['taskStatus'] == 'ERROR' || $result['taskStatus'] == 'WARNING') {
								$debugFile = $this->tmpDir . '/data-load-log.txt';

								// Find upload message
								$uploadMessage = $this->restApi->getUploadMessage($gdJob['pid'], $datasetName);
								if ($uploadMessage) {
									file_put_contents($debugFile, $uploadMessage . PHP_EOL . PHP_EOL, FILE_APPEND);
								}

								// Look for .json and .log files in WebDav folder
								$webDav->saveLogs($tmpFolderName, $debugFile);
								$debug['loadData'] = $this->s3Client->uploadFile($debugFile);

								throw new RestApiException('Load Data Error. ' . $uploadMessage);
							}
						} catch (RestApiException $e) {
							throw new RestApiException('ETL load failed: ' . $e->getMessage());
						}

						break;
				}

				if ($this->clToolApi->debugLogUrl) {
					$debug[$gdJob['command']] = $this->clToolApi->debugLogUrl;
				}
				$output .= $this->clToolApi->output;
			}
		} catch (CLToolApiErrorException $e) {
			$error = 'CL Tool Error: ' . $e->getMessage();
		} catch (RestApiException $e) {
			$error = $e->getMessage();
		} catch (WebDavException $e) {
			$error = 'WebDav error: ' . $e->getMessage();
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
			'gdWriteStartTime' => $gdWriteStartTime,
			'csvFile' => $this->tmpDir . '/data.csv'
		);
		if ($error) {
			$result['error'] = $error;
		}
		return $this->_prepareResult($job['id'], $result, $output);
	}


}
