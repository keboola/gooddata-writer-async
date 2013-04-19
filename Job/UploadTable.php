<?php
/**
 * @author Jakub Matejka <jakub@keboola.com>
 * @date 2013-04-17
 */

namespace Keboola\GoodDataWriter\Job;

use Keboola\GoodDataWriter\Exception\WrongConfigurationException,
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
			exec('curl -s -L ' . escapeshellarg($xmlFile) . ' > ' . $xmlFilePath);
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
				'incremental' => isset($params['incremental']) ? $params['incremental'] :
					(!empty($tableDefinition['incremental']) ? $tableDefinition['incremental'] : 0)
			);
		}

		$incrementalLoad = !empty($params['incremental']) ? $params['incremental'] : null;
		$csvUrl = $this->mainConfig['storageApi.url'] . '/storage/tables/' . $params['tableId'] . '/export?escape=1'
			. ($incrementalLoad ? '&changedSince=-' . $incrementalLoad . '+days' : null);
		$csvFilePath = tempnam($this->tmpDir, 'csv');
		exec('curl --header "X-StorageApi-Token: ' . $job['token'] . '" -s ' . escapeshellarg($csvUrl) . ' > ' . $csvFilePath);
		$csvFile = $csvFilePath;

		$gdWriteStartTime = date('c');
		$debug = array();
		$output = null;
		try {
			$this->clToolApi->setCredentials($this->configuration->bucketInfo['gd']['username'], $this->configuration->bucketInfo['gd']['password']);
			foreach ($gdJobs as $gdJob) {
				$this->clToolApi->debugLogUrl = null;
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
						$this->clToolApi->loadData($gdJob['pid'], $xmlFile, $csvFile, $params['incremental']);
						break;
				}
				if ($this->clToolApi->debugLogUrl) {
					$debug[] = $this->clToolApi->debugLogUrl;
				}
				$output .= $this->clToolApi->output;
			}

			$this->configuration->setTableAttribute($params['tableId'], 'lastExportDate', date('c', $startTime));
			return $this->_prepareResult($job['id'], array(
				'debug' => json_encode($debug),
				'gdWriteStartTime' => $gdWriteStartTime,
				'gdWriteBytes' => filesize($csvFilePath),
				'csvFile' => $csvFilePath
			), $output);

		} catch (CLToolApiErrorException $e) {
			if ($this->clToolApi->debugLogUrl) {
				$debug[] = $this->clToolApi->debugLogUrl;
			}
			$output .= $this->clToolApi->output;
			return $this->_prepareResult($job['id'], array(
				'status' => 'error',
				'error' => $e->getMessage(),
				'debug' => json_encode($debug),
				'gdWriteStartTime' => $gdWriteStartTime,
				'gdWriteBytes' => filesize($csvFilePath),
				'csvFile' => $csvFilePath
			), $output);
		}
	}
}