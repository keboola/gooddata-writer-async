<?php
/**
 * @author Jakub Matejka <jakub@keboola.com>
 * @date 2013-04-12
 */

namespace Keboola\GoodDataWriter\Job;

use Keboola\GoodDataWriter\Exception\JobRunException,
	Keboola\GoodDataWriter\GoodData\CLToolApiErrorException;

class UpdateDataset extends GenericJob
{
	/**
	 * @param $job
	 * @param $params
	 * @throws JobRunException
	 * @return array
	 */
	public function run($job, $params)
	{
		if (empty($job['pid'])) {
			throw new JobRunException("Parameter 'pid' is missing");
		}
		if (empty($job['xmlFile'])) {
			throw new JobRunException("Parameter 'xmlFile' is missing");
		}
		if (!isset($params['updateAll'])) {
			throw new JobRunException("Parameter 'updateAll' is missing");
		}
		$this->configuration->checkGoodDataSetup();

		$xmlFile = $job['xmlFile'];
		if (!is_file($xmlFile)) {
			$xmlFilePath = tempnam(sys_get_temp_dir(), 'xml');
			exec('curl -s ' . escapeshellarg($xmlFile) . ' > ' . $xmlFilePath);
			$xmlFile = $xmlFilePath;
		}

		$gdWriteStartTime = date('c');
		try {
			$this->clToolApi->setCredentials($this->configuration->bucketInfo['gd']['username'], $this->configuration->bucketInfo['gd']['password']);
			$this->clToolApi->updateDataset($job['pid'], $xmlFile, $params['updateAll']);

			return $this->_prepareResult($job['id'], array(
				'debug' => $this->clToolApi->debugLogUrl,
				'gdWriteStartTime' => $gdWriteStartTime
			), $this->clToolApi->output);

		} catch (CLToolApiErrorException $e) {
			return $this->_prepareResult($job['id'], array(
				'status' => 'error',
				'error' => $e->getMessage(),
				'debug' => $this->clToolApi->debugLogUrl,
				'gdWriteStartTime' => $gdWriteStartTime
			), $this->clToolApi->output);
		}

	}
}