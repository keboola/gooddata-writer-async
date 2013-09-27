<?php
/**
 * @author Jakub Matejka <jakub@keboola.com>
 * @date 2013-05-07
 */

namespace Keboola\GoodDataWriter\Job;

use Keboola\GoodDataWriter\Exception\WrongConfigurationException,
	Keboola\GoodDataWriter\GoodData\RestApiException,
	Keboola\GoodDataWriter\GoodData\UnauthorizedException;

class DropDataset extends AbstractJob
{
	/**
	 * @param $job
	 * @param $params
	 * @throws WrongConfigurationException
	 * @return array
	 */
	public function run($job, $params)
	{
		if (empty($job['dataset'])) {
			throw new WrongConfigurationException("Parameter 'dataset' is missing");
		}
		if (empty($params['tableId'])) {
			throw new WrongConfigurationException("Parameter 'tableId' is missing");
		}
		$this->configuration->checkGoodDataSetup();

		$projects = $this->configuration->getProjects();
		$gdWriteStartTime = date('c');

		try {
			$this->restApi->setCredentials($this->configuration->bucketInfo['gd']['username'], $this->configuration->bucketInfo['gd']['password']);
			$this->restApi->getProject($this->configuration->bucketInfo['gd']['pid']);

			foreach ($projects as $project) if ($project['active']) {
				$this->restApi->dropDataset($project['pid'], $job['dataset']);
			}

			$this->configuration->setTableAttribute($params['tableId'], 'lastExportDate', '');
			return $this->_prepareResult($job['id'], array(
				'gdWriteStartTime' => $gdWriteStartTime
			), $this->restApi->callsLog());
		} catch (UnauthorizedException $e) {
			throw new WrongConfigurationException('Rest API Login failed');
		} catch (RestApiException $e) {
			return $this->_prepareResult($job['id'], array(
				'status' => 'error',
				'error' => $e->getMessage(),
				'gdWriteStartTime' => $gdWriteStartTime
			), $this->restApi->callsLog());
		}
	}
}