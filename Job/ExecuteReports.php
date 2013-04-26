<?php
/**
 * @author Jakub Matejka <jakub@keboola.com>
 * @date 2013-04-12
 */

namespace Keboola\GoodDataWriter\Job;

use Keboola\GoodDataWriter\Exception\WrongConfigurationException,
	Keboola\GoodDataWriter\GoodData\CLToolApiErrorException;

class ExecuteReports extends GenericJob
{
	/**
	 * @param $job
	 * @param $params
	 * @throws WrongConfigurationException
	 * @return array
	 */
	public function run($job, $params)
	{
		$this->configuration->checkGoodDataSetup();

		$pids = array();
		if (empty($job['pid'])) {
			$projects = $this->configuration->getProjects();
			foreach ($projects as $project) if ($project['active']) {
				$pids[] = $project['pid'];
			}
		} else {
			$pids[] = $job['pid'];
		}


		$gdWriteStartTime = date('c');
		try {
			$this->clToolApi->setCredentials($this->configuration->bucketInfo['gd']['username'], $this->configuration->bucketInfo['gd']['password']);
			foreach ($pids as $pid) {
				$this->clToolApi->executeReports($pid);
			}

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