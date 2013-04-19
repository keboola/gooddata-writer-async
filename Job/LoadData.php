<?php
/**
 * @author Jakub Matejka <jakub@keboola.com>
 * @date 2013-04-12
 */

namespace Keboola\GoodDataWriter\Job;

use Keboola\GoodDataWriter\Exception\WrongConfigurationException,
	Keboola\GoodDataWriter\GoodData\CLToolApiErrorException;

class LoadData extends GenericJob
{
	/**
	 * @param $job
	 * @param $params
	 * @throws WrongConfigurationException
	 * @return array
	 */
	public function run($job, $params)
	{
		if (empty($job['pid'])) {
			throw new WrongConfigurationException("Parameter 'pid' is missing");
		}
		if (empty($job['xmlFile'])) {
			throw new WrongConfigurationException("Parameter 'xmlFile' is missing");
		}
		if (!isset($params['incremental'])) {
			throw new WrongConfigurationException("Parameter 'incremental' is missing");
		}
		$this->configuration->checkGoodDataSetup();

		$xmlFile = $job['xmlFile'];
		if (!is_file($xmlFile)) {
			$xmlFilePath = tempnam($this->tmpDir, 'xml');
			exec('curl -s ' . escapeshellarg($xmlFile) . ' > ' . $xmlFilePath);
			$xmlFile = $xmlFilePath;
		}

		$incrementalLoad = !empty($params['incremental']) ? $params['incremental'] : null;

		$csvUrl = $job['sapiUrl'] . '/storage/tables/' . $params['tableId'] . '/export?escape=1'
			. ($incrementalLoad ? '&changedSince=-' . $incrementalLoad . '+days' : null);
		$csvFilePath = tempnam($this->tmpDir, 'csv');
		exec('curl --header "X-StorageApi-Token: ' . $job['token'] . '" -s ' . escapeshellarg($csvUrl) . ' > ' . $csvFilePath);
		$csvFile = $csvFilePath;


		$gdWriteStartTime = date('c');
		try {
			$this->clToolApi->setCredentials($this->configuration->bucketInfo['gd']['username'], $this->configuration->bucketInfo['gd']['password']);
			$this->clToolApi->loadData($job['pid'], $xmlFile, $csvFile, $params['incremental']);

			return $this->_prepareResult($job['id'], array(
				'debug' => $this->clToolApi->debugLogUrl,
				'gdWriteStartTime' => $gdWriteStartTime,
				'csvFile' => $csvFilePath
			), $this->clToolApi->output);

		} catch (CLToolApiErrorException $e) {
			return $this->_prepareResult($job['id'], array(
				'status' => 'error',
				'error' => $e->getMessage(),
				'debug' => $this->clToolApi->debugLogUrl,
				'gdWriteStartTime' => $gdWriteStartTime,
				'csvFile' => $csvFilePath
			), $this->clToolApi->output);
		}

	}
}