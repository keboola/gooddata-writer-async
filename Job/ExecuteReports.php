<?php
/**
 * @author Jakub Matejka <jakub@keboola.com>
 * @date 2013-04-12
 */

namespace Keboola\GoodDataWriter\Job;

use Keboola\GoodDataWriter\Exception\JobRunException,
	Keboola\GoodDataWriter\GoodData\CLToolApiErrorException;

class ExecuteReports extends GenericJob
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
		$this->configuration->checkGoodDataSetup();


		$gdWriteStartTime = date('c');
		try {
			$this->clToolApi->setCredentials($this->configuration->bucketInfo['gd']['username'], $this->configuration->bucketInfo['gd']['password']);
			$this->clToolApi->executeReports($job['pid']);

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