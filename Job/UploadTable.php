<?php
/**
 * @author Jakub Matejka <jakub@keboola.com>
 * @date 2013-04-17
 */

namespace Keboola\GoodDataWriter\Job;

use Keboola\GoodDataWriter\Exception\WrongConfigurationException,
	Keboola\GoodDataWriter\Exception\JobProcessException,
	Keboola\GoodDataWriter\GoodData\CLToolApiErrorException;
use Keboola\GoodDataWriter\GoodData\RestApiException;

class UploadTable extends GenericJob
{
	public function run($job, $params)
	{
		if (empty($job['xmlFile'])) {
			throw new WrongConfigurationException("Parameter 'xmlFile' is missing");
		}
		$this->configuration->checkGoodDataSetup();

		$startTime = time();
		$tmpFolderName = $job['id'] . '-' . uniqid();
		$tmpFolder = $this->tmpDir . '/' . $tmpFolderName;
		mkdir($tmpFolder);

		$xmlFile = $job['xmlFile'];
		if (!is_file($xmlFile)) {
			$xmlFilePath = $tmpFolder . '/model.xml';
			exec('curl -s -L ' . escapeshellarg($xmlFile) . ' > ' . escapeshellarg($xmlFilePath));
			$xmlFile = $xmlFilePath;
		}

		libxml_use_internal_errors(TRUE);
		$xmlFileObject = simplexml_load_file($xmlFile);
		if (!$xmlFileObject) {
			$errors = '';
			foreach (libxml_get_errors() as $error) {
				$errors .= $error->message;
			}
			return $this->_prepareResult($job['id'], array(
				'status' => 'error',
				'error' => $errors,
				'debug' => $this->clToolApi->debugLogUrl
			), $this->clToolApi->output);
		}

		$projects = $this->configuration->getProjects();
		$gdJobs = array();

		// Create used date dimensions
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
				$this->configuration->setDateDimensionAttribute($dimension, 'lastExportDate', date('c', $startTime));
			}
		}

		$tableDefinition = $this->configuration->getTableDefinition($params['tableId']);
		$incrementalLoad = (isset($params['incrementalLoad'])) ? $params['incrementalLoad']
			: (!empty($tableDefinition['incrementalLoad']) ? $tableDefinition['incrementalLoad'] : 0);
		$sanitize = (isset($params['sanitize'])) ? $params['sanitize']
			: empty($tableDefinition['sanitize']);

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

		$sapiClient = new \Keboola\StorageApi\Client(
			$job['token'],
			$this->mainConfig['storageApi.url'],
			$this->mainConfig['user_agent']
		);

		$options = array('format' => 'escaped');
		if ($incrementalLoad) {
			$options['changedSince'] = '-' . $incrementalLoad . ' days';
		}
		$sapiClient->exportTable($params['tableId'], $tmpFolder . '/data.csv', $options);

		if ($sanitize) {
			rename($tmpFolder . '/data.csv', $tmpFolder . '/data.csv.in');
			$nullReplace = 'cat ' . escapeshellarg($tmpFolder . '/data.csv.in') . ' | sed \'s/\"NULL\"/\"\"/g\' | awk -v OFS="\",\"" -F"\",\"" \'{';

			$i = 1;
			$columnsCount = $xmlFileObject->columns->column->count();
			foreach ($xmlFileObject->columns->column as $column) {
				$type = (string)$column->ldmType;
				$value = NULL;
				switch ($type) {
					case 'ATTRIBUTE':
						$value = '-- empty --';
						break;
					case 'LABEL':
					case 'FACT':
						$value = '0';
						break;
					case 'DATE':
						$format = (string)$column->format;
						$value = str_replace(
							array('yyyy', 'MM', 'dd', 'hh', 'HH', 'mm', 'ss', 'kk'),
							array('1900', '01', '01', '00', '00', '00', '00', '00'),
							$format);
						break;
				}
				if (!is_null($value)) {
					$testValue = '""';
					if ($i == 1) {
						$testValue = '"\""';
						$value = '\"' . $value;
					}
					if ($i == $columnsCount) {
						$testValue = '"\""';
						$value .= '\"';
					}
					$nullReplace .= 'if ($' . $i . ' == ' . $testValue . ') {$' . $i . ' = "' . $value . '"} ';
				}
				$i++;
			}
			$nullReplace .= '; print }\' > ' . escapeshellarg($tmpFolder . '/data.csv');
			shell_exec($nullReplace);
			if (!file_exists($tmpFolder . '/data.csv.in')) {
				throw new JobProcessException(sprintf("CSV sanitization failed. Job id is '%s'", $tmpFolderName));
			}
			unlink($tmpFolder . '/data.csv.in');
		}


		// Remove ignored columns from csv
		$ignoredColumns = array();
		$dateColumnNames = array();
		$dateColumns = array();
		$referenceColumns = array();
		$i = 1;
		foreach ($xmlFileObject->columns->column as $column) {
			if ((string)$column->ldmType == 'IGNORE') {
				$ignoredColumns[] = $i;
			} elseif ((string)$column->ldmType == 'DATE') {
				$dateColumns[] = $i;
				$dateColumnNames[] = (string)$column->name;
			} elseif ((string)$column->ldmType == 'REFERENCE') {
				$referenceColumns[mb_strtolower(str_replace(' ', '', (string)$column->schemaReference))] = (string)$column->name;
			}
			$i++;
		}

		$gdWriteStartTime = date('c');
		$debug = array();
		$output = null;

		$datasetName = mb_strtolower(str_replace(' ', '', $xmlFileObject->name));
		$this->restApi->login($this->configuration->bucketInfo['gd']['username'], $this->configuration->bucketInfo['gd']['password']);

		$error = false;
		$this->clToolApi->setCredentials($this->configuration->bucketInfo['gd']['username'], $this->configuration->bucketInfo['gd']['password']);
		foreach ($gdJobs as $gdJob) {
			$this->clToolApi->debugLogUrl = null;
			try {
				switch ($gdJob['command']) {
					case 'createDate':
						$this->clToolApi->createDate($gdJob['pid'], $gdJob['name'], $gdJob['includeTime']);
						break;
					case 'createDataset':
						$this->clToolApi->createDataset($gdJob['pid'], $xmlFile);
						break;
					case 'updateDataset':
						$this->clToolApi->updateDataset($gdJob['pid'], $xmlFile, 1);

						break;
					case 'loadData':

						// Get manifest
						$manifest = $this->restApi->get(sprintf('/gdc/md/%s/ldm/singleloadinterface/dataset.%s/manifest', $gdJob['pid'], $datasetName));
						$columns = array();

						foreach ($manifest['dataSetSLIManifest']['parts'] as &$column) {
							if ($incrementalLoad) {
								$column['mode'] = 'INCREMENTAL';
							}

							if (substr($column['columnName'], 0, strlen($datasetName) + 5) == 'f_' . $datasetName . '.f_') {
								// Facts have id "f_DATASET.f_COLUMN"
								$columns[substr($column['columnName'], strlen($datasetName) + 5)] = array(
									'name' => $column['columnName'],
									'type' => 'FACT',
									'manifest' => $column
								);
							} elseif (substr($column['columnName'], 0, strlen($datasetName) + 6) == 'f_' . $datasetName . '.nm_') {
								// Connection points have id "f_DATASET.nm_COLUMN"
								$columns[substr($column['columnName'], strlen($datasetName) + 6)] = array(
									'name' => $column['columnName'],
									'type' => 'CONNECTION_POINT',
									'manifest' => $column
								);
							} elseif (substr($column['columnName'], 0, strlen($datasetName) + 6) == 'f_' . $datasetName . '.dt_') {
								// Dates have id "f_DATASET.dt_COLUMN" and date facts have id "f_DATASET.dt_COLUMN_id"
								$dateColumnName = substr($column['columnName'], strrpos($column['columnName'], '.dt_') + 4);

								if (in_array($dateColumnName, $dateColumnNames)) {
									// date itself
									if (!isset($columns[$dateColumnName])) {
										$columns[$dateColumnName] = array(
											'type' => 'DATE'
										);
									}
									$columns[$dateColumnName]['manifest'] = $column;
									$columns[$dateColumnName]['name'] = $column['columnName'];
								} elseif (strpos($dateColumnName, '_id') !== false) {
									// date fact
									$dateColumnName = substr($dateColumnName, 0, strpos($dateColumnName, '_id'));
									if (in_array($dateColumnName, $dateColumnNames)) {
										if (!isset($columns[$dateColumnName])) {
											$columns[$dateColumnName] = array(
												'type' => 'DATE'
											);
										}
										$columns[$dateColumnName]['date_manifest'] = $column;
										$columns[$dateColumnName]['date_name'] = $column['columnName'];
									} else {
										throw new JobProcessException(sprintf("Referenced date '%s' has not been found in writer", $dateColumnName));
									}
								} else {
									throw new JobProcessException(sprintf("Referenced date '%s' has not been found in writer", $dateColumnName));
								}
							} elseif (substr($column['columnName'], 0, strlen($datasetName) + 3) == 'd_' . $datasetName . '_') {
								// Attributes and labels have id "d_DATASET_COLUMN.nm_COLUMN"
								$columns[substr($column['columnName'], strrpos($column['columnName'], '.nm_') + 4)] = array(
									'name' => $column['columnName'],
									'type' => 'ATTRIBUTE',
									'manifest' => $column
								);
							} elseif (substr($column['columnName'], 0, 2) == 'f_' && strpos($column['columnName'], '.nm_') !== false) {
								// References
								$reference = substr($column['columnName'], 2, strpos($column['columnName'], '.nm_')-2);
								if (isset($referenceColumns[$reference])) {
									$columns[$referenceColumns[$reference]] = array(
										'name' => $column['columnName'],
										'type' => 'REFERENCE',
										'manifest' => $column
									);
								} else {
									throw new JobProcessException(sprintf("Referenced dataset '%s' has not been found in writer", $reference));
								}
							} else {
								throw new JobProcessException(sprintf("Column '%s' from GoodData project has not been found in dataset", $column['columnName']));
							}
						}

						// Reorder manifest columns by csv
						$manifestColumns = array();
						$manifestColumnNames = array();
						$i = 0;
						foreach ($xmlFileObject->columns->column as $column) if ((string)$column->ldmType != 'IGNORE') {
							$columnName = mb_strtolower(str_replace(' ', '', (string)$column->name));
							if (isset($columns[$columnName])) {
								$manifestColumns[] = $columns[$columnName]['manifest'];
								$manifestColumnNames[] = $columns[$columnName]['name'];
								if (isset($columns[$columnName]['date_manifest'])) {
									$manifestColumns[] = $columns[$columnName]['date_manifest'];
								}
							} else {
								throw new JobProcessException(sprintf("Column '%s' has not been found in GoodData project", (string)$column->name));
							}
							$i++;
						}
						$manifest['dataSetSLIManifest']['parts'] = $manifestColumns;
						$manifest['dataSetSLIManifest']['file'] = 'data.csv';


						// Add column headers according to manifest, calculate date facts and remove ignored columns
						rename($tmpFolder . '/data.csv', $tmpFolder . '/data.csv.1');
						$command  = 'cat ' . escapeshellarg($tmpFolder . '/data.csv.1') . ' | php ' . escapeshellarg($this->rootPath . '/GoodData/convert_csv.php');
						$command .= ' -h' . implode(',', $manifestColumnNames);
						if (count($dateColumns)) {
							$command .= ' -d' . implode(',', $dateColumns);
						}
						if (count($ignoredColumns)) {
							$command .= ' -i' . implode(',', $ignoredColumns);
						}
						$command .= ' > ' . escapeshellarg($tmpFolder . '/data.csv');
						shell_exec($command);
						if (!file_exists($tmpFolder . '/data.csv')) {
							throw new JobProcessException(sprintf("CSV preparation failed. Job id is '%s'", $tmpFolderName));
						}
						unlink($tmpFolder . '/data.csv.1');


						// Send data to WebDav
						file_put_contents($tmpFolder . '/upload_info.json', json_encode($manifest));
						shell_exec('zip -j ' . escapeshellarg($tmpFolder . '/upload.zip') . ' ' . escapeshellarg($tmpFolder . '/upload_info.json') . ' ' . escapeshellarg($tmpFolder . '/data.csv'));
						shell_exec(sprintf('curl -i --insecure -X MKCOL -v https://%s:%s@secure-di.gooddata.com/uploads/%s/',
							urlencode($this->configuration->bucketInfo['gd']['username']), $this->configuration->bucketInfo['gd']['password'], $tmpFolderName));
						shell_exec(sprintf('curl -i --insecure -X PUT --data-binary @%s -v https://%s:%s@secure-di.gooddata.com/uploads/%s/upload.zip',
							$tmpFolder . '/upload.zip', urlencode($this->configuration->bucketInfo['gd']['username']), $this->configuration->bucketInfo['gd']['password'], $tmpFolderName));

						// Run load task
						try {
							$this->restApi->loadData($gdJob['pid'], $tmpFolderName);
						} catch (RestApiException $e) {
							throw new JobProcessException('ETL load failed: ' . $e->getMessage());
						}

						break;
				}
			} catch (CLToolApiErrorException $e) {
				$this->clToolApi->output .= '!!! ERROR !!' . PHP_EOL . $e->getMessage() . PHP_EOL;
				$error = true;
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
		return $this->_prepareResult($job['id'], array(
			'status' => $error ? 'error' : 'success',
			'debug' => json_encode($debug),
			'gdWriteStartTime' => $gdWriteStartTime,
			'gdWriteBytes' => filesize($tmpFolder . '/data.csv'),
			'csvFile' => $tmpFolder . '/data.csv'
		), $output);
	}
}
