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
use Keboola\GoodDataWriter\GoodData\WebDav;
use Keboola\StorageApi\Client as StorageApiClient;

class UploadTable extends GenericJob
{
	public function run($job, $params)
	{
		if (empty($job['xmlFile'])) {
			throw new WrongConfigurationException("Parameter 'xmlFile' is missing");
		}
		$this->configuration->checkGoodDataSetup();

		$startTime = time();
		$tmpFolderName = basename($this->tmpDir);
		$csvHandler = new CsvHandler($this->rootPath, $this->s3Client, $this->tmpDir);
		$projects = $this->configuration->getProjects();
		$gdJobs = array();


		// Get xml
		$xmlFile = $job['xmlFile'];
		if (!is_file($xmlFile)) {
			$xmlFile = $csvHandler->downloadXml($xmlFile);
		}
		$xmlFileObject = $csvHandler->getXml($xmlFile);


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
		$tableDefinition = $this->configuration->getTableDefinition($params['tableId']);
		$incrementalLoad = (isset($params['incrementalLoad'])) ? $params['incrementalLoad']
			: (!empty($tableDefinition['incrementalLoad']) ? $tableDefinition['incrementalLoad'] : 0);
		$sanitize = (isset($params['sanitize'])) ? $params['sanitize']
			: !empty($tableDefinition['sanitize']);

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
				'pid' => $project['pid']
			);
		}


		// Get csv
		$sapiClient = new StorageApiClient(
			$job['token'],
			$this->mainConfig['storageApi.url'],
			$this->mainConfig['user_agent']
		);
		$options = array('format' => 'escaped');
		if ($incrementalLoad) {
			$options['changedSince'] = '-' . $incrementalLoad . ' days';
		}
		$sapiClient->exportTable($params['tableId'], $this->tmpDir . '/data.csv', $options);


		$datasetName = $this->_gdName($xmlFileObject->name);



		// Start GoodData transfer
		$gdWriteStartTime = date('c');
		$this->restApi->setCredentials($this->configuration->bucketInfo['gd']['username'], $this->configuration->bucketInfo['gd']['password']);


		// Prepare manifest and csv
		if ($sanitize) {
			$csvHandler->sanitize($xmlFileObject, $this->tmpDir . '/data.csv');
		}
		$manifest = $csvHandler->getManifest($xmlFileObject, $incrementalLoad);
		$csvHandler->prepareCsv($xmlFileObject, $this->tmpDir . '/data.csv');

		file_put_contents($this->tmpDir . '/upload_info.json', json_encode($manifest));
		$csvFileSize = filesize($this->tmpDir . '/data.csv');


		// Upload csv
		$webdavUrl = null;
		if (isset($this->configuration->bucketInfo['gd']['backendUrl'])) {
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
		$webDav = new WebDav($this->configuration->bucketInfo['gd']['username'], $this->configuration->bucketInfo['gd']['password'], $webdavUrl);
		$webDav->upload($this->tmpDir, $tmpFolderName, 'upload_info.json', 'data.csv');


		// Execute enqueued jobs
		$debug = array();
		$output = null;
		$error = false;
		foreach ($gdJobs as $gdJob) {
			try {
				switch ($gdJob['command']) {
					case 'createDate':

						$tmpFolderDimension = $this->tmpDir . '/' . $this->_gdName($gdJob['name']);
						mkdir($tmpFolderDimension);
						$tmpFolderNameDimension = $tmpFolderName . '-' . $this->_gdName($gdJob['name']);

						$this->restApi->createDateDimension($gdJob['pid'], $gdJob['name'], $gdJob['includeTime']);

						$timeDimensionManifest = $csvHandler->getTimeDimensionManifest($gdJob['name']);
						file_put_contents($tmpFolderDimension . '/upload_info.json', $timeDimensionManifest);
						copy($this->rootPath . '/GoodData/time-dimension.csv', $tmpFolderDimension . '/data.csv');
						$csvFileSize += filesize($tmpFolderDimension . '/data.csv');
						$webDav->upload($tmpFolderDimension, $tmpFolderNameDimension, 'upload_info.json', 'data.csv');

						$result = $this->restApi->loadData($gdJob['pid'], $tmpFolderNameDimension);
						if ($result['taskStatus'] == 'ERROR' || $result['taskStatus'] == 'WARNING') {
							$debugFile = $tmpFolderDimension . '/data-load-log.txt';

							// Find upload message
							$uploadMessage = $this->restApi->getUploadMessage($gdJob['pid'], 'time.' . $this->_gdName($gdJob['name']));
							if ($uploadMessage) {
								file_put_contents($debugFile, $uploadMessage . PHP_EOL . PHP_EOL, FILE_APPEND);
							}

							// Look for .json and .log files in WebDav folder
							$webDav->saveLogs($tmpFolderNameDimension, $debugFile);
							$debug['timeDimension'] = $this->s3Client->uploadFile($debugFile);

						}

						$this->configuration->setDateDimensionAttribute($gdJob['name'], 'lastExportDate', date('c', $startTime));

						break;
					case 'createDataset':
						$this->clToolApi->debugLogUrl = null;
						$this->clToolApi->setCredentials($this->configuration->bucketInfo['gd']['username'], $this->configuration->bucketInfo['gd']['password']);
						$this->clToolApi->createDataset($gdJob['pid'], $xmlFile);
						break;
					case 'updateDataset':
						$this->clToolApi->debugLogUrl = null;
						$this->clToolApi->setCredentials($this->configuration->bucketInfo['gd']['username'], $this->configuration->bucketInfo['gd']['password']);
						$this->clToolApi->updateDataset($gdJob['pid'], $xmlFile, 1);

						break;
					case 'loadData':

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
							}
						} catch (RestApiException $e) {
							throw new JobProcessException('ETL load failed: ' . $e->getMessage());
						}

						break;
				}
			} catch (CLToolApiErrorException $e) {
				return $this->_prepareResult($job['id'], array(
					'status' => 'error',
					'error' => $e->getMessage(),
					'gdWriteStartTime' => $gdWriteStartTime
				), $this->clToolApi->output);
			} catch (UnauthorizedException $e) {
				throw new WrongConfigurationException('Rest API Login failed');
			} catch (RestApiException $e) {echo $e->getMessage().PHP_EOL.$e->getTraceAsString();
				return $this->_prepareResult($job['id'], array(
					'status' => 'error',
					'error' => $e->getMessage(),
					'gdWriteStartTime' => $gdWriteStartTime
				), $this->restApi->callsLog());
			}

			if ($this->clToolApi->debugLogUrl) {
				$debug[$gdJob['command']] = $this->clToolApi->debugLogUrl;
			}
			$output .= $this->clToolApi->output;
		}

		if (empty($tableDefinition['lastExportDate'])) {
			$this->configuration->setTableAttribute($params['tableId'], 'export', 1);
		}
		$this->configuration->setTableAttribute($params['tableId'], 'lastExportDate', date('c', $startTime));
		$result = array(
			'status' => $error ? 'error' : 'success',
			'debug' => json_encode($debug),
			'gdWriteStartTime' => $gdWriteStartTime,
			'gdWriteBytes' => $csvFileSize,
			'csvFile' => $this->tmpDir
		);
		if ($error && $error !== true) {
			$result['error'] = $error;
		}
		return $this->_prepareResult($job['id'], $result, $output);
	}


	private function _gdName($name)
	{
		return preg_replace('/[^a-z\d ]/i', '', $name);
	}
}
