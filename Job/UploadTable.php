<?php
/**
 * @author Jakub Matejka <jakub@keboola.com>
 * @date 2013-04-17
 */

namespace Keboola\GoodDataWriter\Job;

use Keboola\GoodDataWriter\Exception\WrongConfigurationException,
	Keboola\GoodDataWriter\Exception\JobProcessException,
	Keboola\GoodDataWriter\GoodData\CLToolApiErrorException;

class UploadTable extends GenericJob
{
	public function run($job, $params)
	{
		if (empty($job['xmlFile'])) {
			throw new WrongConfigurationException("Parameter 'xmlFile' is missing");
		}
		$this->configuration->checkGoodDataSetup();

		$startTime = time();

		$xmlFile = $job['xmlFile'];
		if (!is_file($xmlFile)) {
			$xmlFilePath = tempnam($this->tmpDir, 'xml');
			exec('curl -s -L ' . escapeshellarg($xmlFile) . ' > ' . escapeshellarg($xmlFilePath));
			$xmlFile = $xmlFilePath;
		}

		$projects = $this->configuration->getProjects();
		$gdJobs = array();

		// Create used date dimensions
		$dateDimensions = null;
		$xmlObject = simplexml_load_file($xmlFile);
		foreach ($xmlObject->columns->column as $column) if ((string)$column->ldmType == 'DATE') {
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
		$incremental = (isset($params['incremental'])) ? $params['incremental']
			: (!empty($tableDefinition['incremental']) ? $tableDefinition['incremental'] : 0);
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
				'pid' => $project['pid'],
				'incremental' => $incremental
			);
		}


		$csvUrl = $this->mainConfig['storageApi.url'] . '/storage/tables/' . $params['tableId'] . '/export?escape=1'
			. ($incremental ? '&changedSince=-' . $incremental . '+days' : null);
		$csvFilePath = tempnam($this->tmpDir, 'csv');
		exec('curl --header "X-StorageApi-Token: ' . $job['token'] . '" -s ' . escapeshellarg($csvUrl) . ' > ' . $csvFilePath);
		chmod($csvFilePath, 0644);
		$csvFile = $csvFilePath;

		if ($sanitize) {
			libxml_use_internal_errors(TRUE);
			$sxml = simplexml_load_file($xmlFile);
			if ($sxml) {
				$nullReplace = 'cat ' . $csvFile . ' | sed \'s/\"NULL\"/\"\"/g\' | awk -v OFS="\",\"" -F"\",\"" \'{';

				$i = 1;
				$columnsCount = $sxml->columns->column->count();
				foreach ($sxml->columns->column as $column) {
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
				$nullReplace .= '; print }\' > ' . $csvFile . '.out';
				shell_exec($nullReplace);

				$csvFile .= '.out';
			} else {
				$errors = '';
				foreach (libxml_get_errors() as $error) {
					$errors .= $error->message;
				}
				return $this->_prepareResult($job['id'], array(
					'status' => 'error',
					'error' => $errors,
					'debug' => $this->clToolApi->debugLogUrl,
					'csvFile' => $csvFilePath
				), $this->clToolApi->output);
			}
		}


		$gdWriteStartTime = date('c');
		$debug = array();
		$output = null;

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
						$this->clToolApi->loadData($gdJob['pid'], $xmlFile, $csvFile, $incremental);
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

		$this->configuration->setTableAttribute($params['tableId'], 'lastExportDate', date('c', $startTime));
		return $this->_prepareResult($job['id'], array(
			'status' => $error ? 'error' : 'success',
			'debug' => json_encode($debug),
			'gdWriteStartTime' => $gdWriteStartTime,
			'gdWriteBytes' => filesize($csvFilePath),
			'csvFile' => $csvFilePath
		), $output);
	}
}