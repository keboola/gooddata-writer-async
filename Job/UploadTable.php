<?php
/**
 * @author Jakub Matejka <jakub@keboola.com>
 * @date 2013-04-17
 */

namespace Keboola\GoodDataWriter\Job;

use Keboola\GoodDataWriter\Exception\ClientException;
use Keboola\GoodDataWriter\Exception\WrongConfigurationException,
	Keboola\GoodDataWriter\Exception\JobProcessException,
	Keboola\GoodDataWriter\GoodData\CLToolApiErrorException,
	Keboola\GoodDataWriter\GoodData\RestApiException,
	Keboola\GoodDataWriter\Writer\Process,
	Keboola\GoodDataWriter\Writer\ProcessException;

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
			$xmlUrl = $xmlFile;
			$url = parse_url($xmlFile);
			if (empty($url['host'])) {
				$xmlUrl = $this->s3Client->url($xmlFile);
			}
			$xmlFilePath = $tmpFolder . '/model.xml';
			exec('curl -s -L ' . escapeshellarg($xmlUrl) . ' > ' . escapeshellarg($xmlFilePath));
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


		// Prepare model alteration jobs
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


		// Sanitize CSV
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


		// Start GD load
		$gdWriteStartTime = date('c');
		$this->restApi->login($this->configuration->bucketInfo['gd']['username'], $this->configuration->bucketInfo['gd']['password']);
		$this->clToolApi->setCredentials($this->configuration->bucketInfo['gd']['username'], $this->configuration->bucketInfo['gd']['password']);


		// Prepare manifest for data load
		$datasetName = $this->_gdName($xmlFileObject->name);
		$manifest = array(
			'dataSetSLIManifest' => array(
				'file' => 'data.csv',
				'dataSet' => 'dataset.' . $datasetName,
				'parts' => array()
			)
		);
		$csvHeaders = array();
		$ignoredColumnsIndices = array();
		$dateColumnsIndices = array();
		$timeColumnsIndices = array();
		$i = 1;
		foreach ($xmlFileObject->columns->column as $column) {
			$columnName = $this->_gdName($column->name);
			$gdName = null;
			switch ((string)$column->ldmType) {
				case 'CONNECTION_POINT':
					$csvHeaders[] = (string)$column->name;
					$manifest['dataSetSLIManifest']['parts'][] = array(
						'columnName' => (string)$column->name,
						'populates' => array(
							sprintf('label.%s.%s', $datasetName, $columnName)
						),
						'mode' => $incrementalLoad ? 'INCREMENTAL' : 'FULL',
						'referenceKey' => 1
					);
					break;
				case 'FACT':
					$csvHeaders[] = (string)$column->name;
					$manifest['dataSetSLIManifest']['parts'][] = array(
						'columnName' => (string)$column->name,
						'populates' => array(
							sprintf('fact.%s.%s', $datasetName, $columnName)
						),
						'mode' => $incrementalLoad ? 'INCREMENTAL' : 'FULL'
					);
					break;
				case 'ATTRIBUTE':
					$csvHeaders[] = (string)$column->name;
					$manifest['dataSetSLIManifest']['parts'][] = array(
						'columnName' => (string)$column->name,
						'populates' => array(
							sprintf('label.%s.%s', $datasetName, $columnName)
						),
						'mode' => $incrementalLoad ? 'INCREMENTAL' : 'FULL',
						'referenceKey' => 1
					);
					break;
				case 'LABEL':
				case 'HYPERLINK':
					$csvHeaders[] = (string)$column->name;
					$manifest['dataSetSLIManifest']['parts'][] = array(
						'columnName' => (string)$column->name,
						'populates' => array(
							sprintf('label.%s.%s.%s', $datasetName, $this->_gdName($column->reference), $columnName)
						),
						'mode' => $incrementalLoad ? 'INCREMENTAL' : 'FULL'
					);
					break;
				case 'REFERENCE':
					$csvHeaders[] = (string)$column->name;
					$manifest['dataSetSLIManifest']['parts'][] = array(
						'columnName' => (string)$column->name,
						'populates' => array(
							sprintf('label.%s.%s', $this->_gdName($column->schemaReference), $this->_gdName($column->reference))
						),
						'mode' => $incrementalLoad ? 'INCREMENTAL' : 'FULL',
						'referenceKey' => 1
					);
					break;
				case 'DATE':
					$csvHeaders[] = (string)$column->name;
					$manifest['dataSetSLIManifest']['parts'][] = array(
						'columnName' => (string)$column->name,
						'populates' => array(
							sprintf('%s.date.mmddyyyy', $this->_gdName($column->schemaReference))
						),
						'constraints' => array(
							'date' => (string)$column->format
						),
						'mode' => $incrementalLoad ? 'INCREMENTAL' : 'FULL',
						'referenceKey' => 1
					);
					$csvHeaders[] = (string)$column->name . '_dt';
					$manifest['dataSetSLIManifest']['parts'][] = array(
						'columnName' => (string)$column->name . '_dt',
						'populates' => array(
							sprintf('dt.%s.%s', $datasetName, $columnName)
						),
						'mode' => $incrementalLoad ? 'INCREMENTAL' : 'FULL'
					);
					if ((string)$column->datetime == 'true') {
						$csvHeaders[] = (string)$column->name . '_tm';
						$manifest['dataSetSLIManifest']['parts'][] = array(
							'columnName' => (string)$column->name . '_tm',
							'populates' => array(
								sprintf('tm.dt.%s.%s', $datasetName, $columnName)
							),
							'mode' => $incrementalLoad ? 'INCREMENTAL' : 'FULL'
						);
						$csvHeaders[] = (string)$column->name . '_id';
						$manifest['dataSetSLIManifest']['parts'][] = array(
							'columnName' => (string)$column->name . '_id',
							'populates' => array(
								sprintf('label.time.second.of.day.%s', $this->_gdName($column->schemaReference))
							),
							'mode' => $incrementalLoad ? 'INCREMENTAL' : 'FULL',
							'referenceKey' => 1
						);

						$timeColumnsIndices[] = $i;
					}

					$dateColumnsIndices[] = $i;
					break;
				case 'IGNORE':
					$ignoredColumnsIndices[] = $i;
					break;
			}

			$i++;
		}


		$debug = array();
		$output = null;
		$error = false;
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

						// Add column headers according to manifest, calculate date facts and remove ignored columns
						rename($tmpFolder . '/data.csv', $tmpFolder . '/data.csv.1');
						$command  = 'cat ' . escapeshellarg($tmpFolder . '/data.csv.1') . ' | php ' . escapeshellarg($this->rootPath . '/GoodData/convert_csv.php');
						$command .= ' -h' . implode(',', $csvHeaders);
						if (count($dateColumnsIndices)) {
							$command .= ' -d' . implode(',', $dateColumnsIndices);
						}
						if (count($timeColumnsIndices)) {
							$command .= ' -t' . implode(',', $timeColumnsIndices);
						}
						if (count($ignoredColumnsIndices)) {
							$command .= ' -i' . implode(',', $ignoredColumnsIndices);
						}
						$command .= ' > ' . escapeshellarg($tmpFolder . '/data.csv');
						try {
							$output = Process::exec($command);
						} catch (ProcessException $e) {
							throw new JobProcessException(sprintf("CSV preparation failed: %s", $e->getMessage()), NULL, $e);
						}
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


	private function _gdName($name)
	{
		return mb_strtolower(str_replace(' ', '', (string)$name));
	}
}
